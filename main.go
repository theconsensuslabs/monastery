package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"plugin"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gdamore/tcell/v2"
)

func newUUID() string {
	var b [16]byte
	rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type step struct {
	clientID string
	sql      string
	notes    string
}

func parseScript(f *os.File) (preconditions []string, steps []step, err error) {
	scanner := bufio.NewScanner(f)
	seenSeparator := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if line == "---" {
			seenSeparator = true
			continue
		}
		if !seenSeparator {
			preconditions = append(preconditions, line)
			continue
		}
		var notes string
		if idx := strings.Index(line, "--"); idx != -1 {
			notes = strings.TrimSpace(line[idx+2:])
			line = line[:idx]
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			continue
		}
		query := strings.TrimSpace(parts[1])
		if query != "" {
			steps = append(steps, step{strings.TrimSpace(parts[0]), query, notes})
		}
	}
	if !seenSeparator {
		// no separator: everything was steps, not preconditions
		for _, line := range preconditions {
			var notes string
			if idx := strings.Index(line, "--"); idx != -1 {
				notes = strings.TrimSpace(line[idx+2:])
				line = line[:idx]
			}
			parts := strings.SplitN(line, ":", 2)
			if len(parts) < 2 {
				continue
			}
			query := strings.TrimSpace(parts[1])
			if query != "" {
				steps = append(steps, step{strings.TrimSpace(parts[0]), query, notes})
			}
		}
		preconditions = nil
	}
	return preconditions, steps, scanner.Err()
}


// --- logger -----------------------------------------------------------------

type logger struct {
	mu    sync.Mutex
	f     *os.File
	runID string
}

func newLogger(path string, runID string) (*logger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &logger{f: f, runID: runID}, nil
}

func (l *logger) write(event map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	event["run_id"] = l.runID
	b, _ := json.Marshal(event)
	l.f.Write(b)
	l.f.Write([]byte("\n"))
}

func (l *logger) close() {
	l.f.Close()
}

// --- columns ----------------------------------------------------------------

const (
	colClient = iota
	colCommand
	colStart
	colEnd
	colResults
	colError
	colNotes
	numCols
)

var colWidths = [numCols]int{
	10, // client
	60, // command
	14, // start
	14, // end
	20, // results
	40, // error
	50, // notes
}

var colHeaders = [numCols]string{
	"CLIENT", "COMMAND", "STARTED", "ENDED", "RESULTS", "ERROR", "NOTES",
}

var colOffsets [numCols]int

func init() {
	x := 1
	for i, w := range colWidths {
		colOffsets[i] = x
		x += w + 1
	}
}

func totalWidth() int {
	w := 1
	for _, cw := range colWidths {
		w += cw + 1
	}
	return w
}

// --- text wrapping ----------------------------------------------------------

func wrapText(text string, width int) []string {
	if width <= 0 {
		return []string{text}
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{""}
	}
	var lines []string
	for len(runes) > 0 {
		end := width
		if end > len(runes) {
			end = len(runes)
		}
		lines = append(lines, string(runes[:end]))
		runes = runes[end:]
	}
	return lines
}

func rowLines(r rowData) ([numCols][]string, int) {
	fields := [numCols]string{
		r.client, r.command, r.start, r.end, r.results, r.err, r.notes,
	}
	var wrapped [numCols][]string
	maxLines := 1
	for i, f := range fields {
		wrapped[i] = wrapText(f, colWidths[i])
		if len(wrapped[i]) > maxLines {
			maxLines = len(wrapped[i])
		}
	}
	return wrapped, maxLines
}

// --- screen -----------------------------------------------------------------

type rowData struct {
	client  string
	command string
	start   string
	end     string
	results string
	err     string
	notes   string
}

type screen struct {
	mu           sync.Mutex
	s            tcell.Screen
	rows         []rowData
	scrollOffset int
	interactive  bool
	allDone      bool
	dump         string
}

func (sc *screen) captureDump() string {
	w, h := sc.s.Size()
	var sb strings.Builder
	line := make([]rune, w)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			mainc, _, _, _ := sc.s.GetContent(x, y)
			if mainc == 0 {
				mainc = ' '
			}
			line[x] = mainc
		}
		sb.WriteString(strings.TrimRight(string(line), " "))
		sb.WriteRune('\n')
	}
	out := sb.String()
	if idx := strings.LastIndex(out, "┘\n"); idx != -1 {
		out = out[:idx+len("┘\n")]
	}
	return out
}

func newScreen() (*screen, error) {
	s, err := tcell.NewScreen()
	if err != nil {
		return nil, err
	}
	if err := s.Init(); err != nil {
		return nil, err
	}
	s.Clear()
	sc := &screen{s: s}
	sc.drawHeader()
	return sc, nil
}

func (sc *screen) pollEvents(cancel context.CancelFunc, nextStep chan<- struct{}) {
	for {
		ev := sc.s.PollEvent()
		switch ev := ev.(type) {
		case *tcell.EventKey:
			switch ev.Key() {
			case tcell.KeyUp:
				sc.scroll(-1)
			case tcell.KeyDown:
				sc.scroll(1)
			case tcell.KeyEscape:
				sc.mu.Lock()
				sc.dump = sc.captureDump()
				sc.mu.Unlock()
				cancel()
				return
			default:
				if ev.Rune() == 'q' || ev.Rune() == 'Q' {
					sc.mu.Lock()
					sc.dump = sc.captureDump()
					sc.mu.Unlock()
					cancel()
					return
				}
				if sc.interactive && nextStep != nil {
					select {
					case nextStep <- struct{}{}:
					default:
					}
				}
			}
		case *tcell.EventResize:
			sc.mu.Lock()
			sc.s.Sync()
			sc.redrawAll()
			sc.mu.Unlock()
		}
	}
}

func (sc *screen) scroll(delta int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	total := sc.totalContentLines()
	_, h := sc.s.Size()
	visible := h - headerLines
	maxScroll := total - visible
	if maxScroll < 0 {
		maxScroll = 0
	}
	sc.scrollOffset += delta
	if sc.scrollOffset < 0 {
		sc.scrollOffset = 0
	}
	if sc.scrollOffset > maxScroll {
		sc.scrollOffset = maxScroll
	}
	sc.redrawAll()
}

func (sc *screen) totalContentLines() int {
	total := 0
	for _, r := range sc.rows {
		_, n := rowLines(r)
		total += n + 1
	}
	return total
}

func (sc *screen) putStr(x, y int, text string, maxW int, style tcell.Style) {
	runes := []rune(text)
	for i := 0; i < maxW; i++ {
		ch := ' '
		if i < len(runes) {
			ch = runes[i]
		}
		sc.s.SetContent(x+i, y, ch, nil, style)
	}
}

func (sc *screen) drawBorder(y int, left, mid, right rune) {
	w := totalWidth()
	for x := 0; x < w; x++ {
		ch := '─'
		if x == 0 {
			ch = left
		} else if x == w-1 {
			ch = right
		} else {
			cx := 1
			for _, cw := range colWidths {
				cx += cw
				if x == cx {
					ch = mid
					break
				}
				cx++
			}
		}
		sc.s.SetContent(x, y, ch, nil, tcell.StyleDefault)
	}
}

func (sc *screen) drawHRule(y int) {
	sc.drawBorder(y, '├', '┼', '┤')
}

func (sc *screen) drawLogicalRow(y int, r rowData, firstLine int) int {
	defaultStyle := tcell.StyleDefault
	pendingStyle := tcell.StyleDefault.Foreground(tcell.ColorYellow)
	errorStyle := tcell.StyleDefault.Foreground(tcell.ColorRed)
	notesStyle := tcell.StyleDefault

	endStyle := defaultStyle
	if r.end == "pending" {
		endStyle = pendingStyle
	}
	errStyle := defaultStyle
	if r.err != "" {
		errStyle = errorStyle
	}
	styles := [numCols]tcell.Style{
		defaultStyle, defaultStyle, defaultStyle,
		endStyle, defaultStyle, errStyle, notesStyle,
	}

	wrapped, numLines := rowLines(r)
	w := totalWidth()

	drawn := 0
	for line := firstLine; line < numLines; line++ {
		sy := y + drawn
		sc.s.SetContent(0, sy, '│', nil, defaultStyle)
		sc.s.SetContent(w-1, sy, '│', nil, defaultStyle)
		cx := 1
		for _, cw := range colWidths {
			cx += cw
			sc.s.SetContent(cx, sy, '│', nil, defaultStyle)
			cx++
		}
		for col := 0; col < numCols; col++ {
			text := ""
			if line < len(wrapped[col]) {
				text = wrapped[col][line]
			}
			sc.putStr(colOffsets[col], sy, text, colWidths[col], styles[col])
		}
		drawn++
	}
	return drawn
}

func (sc *screen) drawHeader() {
	sc.drawBorder(0, '┌', '┬', '┐')
	w := totalWidth()
	sc.s.SetContent(0, 1, '│', nil, tcell.StyleDefault)
	sc.s.SetContent(w-1, 1, '│', nil, tcell.StyleDefault)
	cx := 1
	for _, cw := range colWidths {
		cx += cw
		sc.s.SetContent(cx, 1, '│', nil, tcell.StyleDefault)
		cx++
	}
	boldStyle := tcell.StyleDefault.Bold(true)
	for i, label := range colHeaders {
		sc.putStr(colOffsets[i], 1, label, colWidths[i], boldStyle)
	}
	sc.drawBorder(2, '╞', '╪', '╡')
}

const headerLines = 3

func (sc *screen) redrawAll() {
	sc.s.Clear()
	sc.drawHeader()
	_, h := sc.s.Size()

	skipped := 0
	for i, r := range sc.rows {
		_, numLines := rowLines(r)
		rowScreenLines := numLines + 1

		if skipped+rowScreenLines <= sc.scrollOffset {
			skipped += rowScreenLines
			continue
		}

		startLine := skipped - sc.scrollOffset
		firstLine := 0
		if startLine < 0 {
			firstLine = -startLine
			startLine = 0
		}
		screenY := headerLines + startLine
		if screenY >= h {
			break
		}

		drawn := sc.drawLogicalRow(screenY, r, firstLine)
		divY := screenY + drawn
		if divY < h {
			if i < len(sc.rows)-1 {
				sc.drawHRule(divY)
			} else {
				sc.drawBorder(divY, '└', '┴', '┘')
			}
		}

		skipped += rowScreenLines
	}

	if sc.scrollOffset > 0 {
		hint := " ↑ more "
		for i, ch := range []rune(hint) {
			sc.s.SetContent(i, headerLines, ch, nil, tcell.StyleDefault.Dim(true))
		}
	}
	total := sc.totalContentLines()
	w, h2 := sc.s.Size()
	if sc.scrollOffset+h2-headerLines < total {
		hint := " ↓ more "
		for i, ch := range []rune(hint) {
			sc.s.SetContent(i, h2-1, ch, nil, tcell.StyleDefault.Dim(true))
		}
	}

	var keyHint string
	if sc.interactive && !sc.allDone {
		keyHint = " any key: next step · q/Esc: quit "
	} else {
		keyHint = " q/Esc: quit "
	}
	hintRunes := []rune(keyHint)
	startX := w - len(hintRunes)
	if startX < 0 {
		startX = 0
	}
	dimStyle := tcell.StyleDefault.Dim(true)
	for i, ch := range hintRunes {
		sc.s.SetContent(startX+i, h2-1, ch, nil, dimStyle)
	}

	sc.s.Show()
}

func (sc *screen) addRow(r rowData) int {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	idx := len(sc.rows)
	sc.rows = append(sc.rows, r)
	sc.redrawAll()
	return idx
}

func (sc *screen) updateRow(idx int, r rowData) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if idx >= len(sc.rows) {
		return
	}
	sc.rows[idx] = r
	sc.redrawAll()
}

func (sc *screen) fini() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.s.Fini()
}

// --- worker -----------------------------------------------------------------

func clientWork(ctx context.Context, id string, conn *sql.Conn, ch <-chan step, sc *screen, lg *logger) {
	for s := range ch {
		start := time.Now()

		lg.write(map[string]any{
			"event":   "query_start",
			"client":  id,
			"command": s.sql,
			"notes":   s.notes,
			"time":    start.Format(time.RFC3339Nano),
		})

		idx := sc.addRow(rowData{
			client:  id,
			command: s.sql,
			start:   start.Format("15:04:05.000"),
			end:     "pending",
			notes:   s.notes,
		})

		rows, err := conn.QueryContext(ctx, s.sql)
		if err != nil {
			end := time.Now()
			lg.write(map[string]any{
				"event":      "query_end",
				"client":     id,
				"command":    s.sql,
				"notes":      s.notes,
				"start_time": start.Format(time.RFC3339Nano),
				"end_time":   end.Format(time.RFC3339Nano),
				"elapsed_ms": end.Sub(start).Milliseconds(),
				"error":      err.Error(),
			})
			sc.updateRow(idx, rowData{
				client:  id,
				command: s.sql,
				start:   start.Format("15:04:05.000"),
				end:     end.Format("15:04:05.000"),
				err:     err.Error(),
				notes:   s.notes,
			})
			continue
		}

		cols, _ := rows.Columns()
		var resultParts []string
		var resultRows [][]any
		var scanErr error
		if len(cols) > 0 {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			for rows.Next() {
				if err := rows.Scan(ptrs...); err != nil {
					scanErr = err
					break
				}
				row := make([]any, len(vals))
				copy(row, vals)
				parts := make([]string, len(row))
				for i, v := range row {
					if b, ok := v.([]byte); ok {
						parts[i] = string(b)
					} else {
						parts[i] = fmt.Sprintf("%v", v)
					}
				}
				resultParts = append(resultParts, "["+strings.Join(parts, " ")+"]")
				resultRows = append(resultRows, row)
			}
		}
		iterErr := rows.Err()
		if err := rows.Close(); err != nil {
			log.Fatalf("unexpected error on close [%s]: %s", id, err)
		}
		if scanErr != nil || iterErr != nil {
			rowErr := scanErr
			if rowErr == nil {
				rowErr = iterErr
			}
			end := time.Now()
			lg.write(map[string]any{
				"event":      "query_end",
				"client":     id,
				"command":    s.sql,
				"notes":      s.notes,
				"start_time": start.Format(time.RFC3339Nano),
				"end_time":   end.Format(time.RFC3339Nano),
				"elapsed_ms": end.Sub(start).Milliseconds(),
				"error":      rowErr.Error(),
			})
			sc.updateRow(idx, rowData{
				client:  id,
				command: s.sql,
				start:   start.Format("15:04:05.000"),
				end:     end.Format("15:04:05.000"),
				err:     rowErr.Error(),
				notes:   s.notes,
			})
			continue
		}

		end := time.Now()
		lg.write(map[string]any{
			"event":      "query_end",
			"client":     id,
			"command":    s.sql,
			"notes":      s.notes,
			"start_time": start.Format(time.RFC3339Nano),
			"end_time":   end.Format(time.RFC3339Nano),
			"elapsed_ms": end.Sub(start).Milliseconds(),
			"columns":    cols,
			"rows":       resultRows,
			"row_count":  len(resultRows),
		})

		sc.updateRow(idx, rowData{
			client:  id,
			command: s.sql,
			start:   start.Format("15:04:05.000"),
			end:     end.Format("15:04:05.000"),
			results: strings.Join(resultParts, "; "),
			notes:   s.notes,
		})
	}
}

// --- main -------------------------------------------------------------------

func defaultPluginDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
}

func main() {
	interval := flag.Duration("interval", 3*time.Second, "delay between dispatching each step (e.g. 500ms, 1s, 1m)")
	interactive := flag.Bool("interactive", false, "wait for any keypress before dispatching each step")
	run := flag.Bool("run", false, "dispatch all steps immediately, dump output, and exit")
	logPath := flag.String("log", "monastery.jsonl", "path to JSON log file")
	pluginDir := flag.String("plugin-dir", defaultPluginDir(), "directory containing driver plugin .so files")
	flag.Parse()

	args := flag.Args()
	if len(args) < 4 {
		log.Fatal("usage: monastery [-interval <duration>] [-interactive] [-log <path>] <driver> <dsn> <isolation level> <script>\n" +
			"  -interval duration  delay between steps (default 3s, e.g. 500ms, 1m)\n" +
			"  -interactive        wait for keypress before each step instead of using interval\n" +
			"  -run                dispatch all steps immediately, dump output, and exit\n" +
			"  -log path           json log file (default monastery.jsonl)\n" +
			"  isolation levels: read-uncommitted, read-committed, repeatable-read, serializable\n" +
			"  drivers: mysql, postgres (loaded from <driver>.so plugin)\n" +
			"  mysql:    root:pass@tcp(127.0.0.1:3306)/dbname\n" +
			"  postgres: host=localhost user=postgres dbname=test sslmode=disable")
	}

	driver, dsn, isolationLevel, scriptPath := args[0], args[1], args[2], args[3]

	pluginPath := filepath.Join(*pluginDir, driver+".so")
	p, err := plugin.Open(pluginPath)
	if err != nil {
		log.Fatalf("load driver plugin %s: %v", pluginPath, err)
	}
	sym, err := p.Lookup("IsolationSQL")
	if err != nil {
		log.Fatalf("plugin %s missing IsolationSQL symbol", pluginPath)
	}
	isolationSQLFn, ok := sym.(func(string) string)
	if !ok {
		log.Fatalf("plugin %s: IsolationSQL has wrong type", pluginPath)
	}
	setSQL := isolationSQLFn(isolationLevel)
	if setSQL == "" {
		log.Fatalf("unknown isolation level %q for driver %s", isolationLevel, driver)
	}

	f, err := os.Open(scriptPath)
	if err != nil {
		log.Fatal("open script:", err)
	}
	defer f.Close()

	preconditions, steps, err := parseScript(f)
	if err != nil {
		log.Fatal("parse script:", err)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.Fatal("open db:", err)
	}
	defer db.Close()

	runID := newUUID()

	lg, err := newLogger(*logPath, runID)
	if err != nil {
		log.Fatal("open log:", err)
	}
	defer lg.close()

	lg.write(map[string]any{
		"event":           "session_start",
		"time":            time.Now().Format(time.RFC3339Nano),
		"driver":          driver,
		"isolation_level": isolationLevel,
		"interval_ms":     interval.Milliseconds(),
	})

	if len(preconditions) > 0 {
		conn, err := db.Conn(context.Background())
		if err != nil {
			log.Fatal("precondition conn:", err)
		}
		for _, sql := range preconditions {
			start := time.Now()
			lg.write(map[string]any{
				"event":   "precondition_start",
				"command": sql,
				"time":    start.Format(time.RFC3339Nano),
				"driver": driver,
			})
			_, execErr := conn.ExecContext(context.Background(), sql)
			end := time.Now()
			event := map[string]any{
				"event":      "precondition_end",
				"command":    sql,
				"start_time": start.Format(time.RFC3339Nano),
				"end_time":   end.Format(time.RFC3339Nano),
				"elapsed_ms": end.Sub(start).Milliseconds(),
			}
			if execErr != nil {
				event["error"] = execErr.Error()
				lg.write(event)
				conn.Close()
				log.Fatalf("precondition %q: %v", sql, execErr)
			}
			lg.write(event)
		}
		conn.Close()
	}

	sc, err := newScreen()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		sc.fini()
		if sc.dump != "" {
			fmt.Print(sc.dump)
		}

		fmt.Println(runID)
	}()
	sc.interactive = *interactive

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var nextStep chan struct{}
	if *interactive {
		nextStep = make(chan struct{}, 1)
	}
	if !*run {
		go sc.pollEvents(cancel, nextStep)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	clientChans := map[string]chan step{}
	var wg sync.WaitGroup

	var workerConnsMu sync.Mutex
	var workerConns []*sql.Conn
	go func() {
		<-ctx.Done()
		workerConnsMu.Lock()
		defer workerConnsMu.Unlock()
		for _, c := range workerConns {
			c.Close()
		}
	}()

	for _, s := range steps {
		if _, ok := clientChans[s.clientID]; !ok {
			conn, err := db.Conn(context.Background())
			if err != nil {
				sc.fini()
				log.Fatal("get conn:", err)
			}
			if _, err := conn.ExecContext(context.Background(), setSQL); err != nil {
				sc.fini()
				log.Fatalf("set isolation level on %s: %v", s.clientID, err)
			}
			workerConnsMu.Lock()
			workerConns = append(workerConns, conn)
			workerConnsMu.Unlock()

			ch := make(chan step, len(steps))
			clientChans[s.clientID] = ch

			wg.Add(1)
			go func(id string, c *sql.Conn, ch chan step) {
				defer wg.Done()
				clientWork(ctx, id, c, ch, sc, lg)
			}(s.clientID, conn, ch)
		}
	}

	if *run {
		for _, s := range steps {
			clientChans[s.clientID] <- s
		}
	} else if *interactive {
		for _, s := range steps {
			clientChans[s.clientID] <- s
			select {
			case <-ctx.Done():
				goto shutdown
			case <-nextStep:
			}
		}
	} else {
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for _, s := range steps {
			clientChans[s.clientID] <- s
			select {
			case <-ctx.Done():
				goto shutdown
			case <-ticker.C:
			}
		}
	}

	for _, ch := range clientChans {
		close(ch)
	}
	wg.Wait()
	lg.write(map[string]any{
		"event": "session_end",
		"time":  time.Now().Format(time.RFC3339Nano),
	})
	if *run {
		sc.mu.Lock()
		sc.dump = sc.captureDump()
		sc.mu.Unlock()
		return
	}
	sc.mu.Lock()
	sc.allDone = true
	sc.redrawAll()
	sc.mu.Unlock()
	<-ctx.Done()
	return

shutdown:
	for _, ch := range clientChans {
		close(ch)
	}
	wg.Wait()
	lg.write(map[string]any{
		"event":  "session_end",
		"time":   time.Now().Format(time.RFC3339Nano),
		"reason": "interrupted",
	})
}
