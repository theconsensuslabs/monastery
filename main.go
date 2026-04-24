package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gdamore/tcell/v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type step struct {
	clientID string
	sql      string
	notes    string
}

func parseScript(f *os.File) ([]step, error) {
	var steps []step
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
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
	return steps, scanner.Err()
}

func isolationSQL(driver, level string) string {
	var cmd string
	var levels map[string]string
	switch driver {
	case "mysql":
		levels = map[string]string{
			"read-uncommitted": "READ UNCOMMITTED",
			"read-committed":   "READ COMMITTED",
			"repeatable-read":  "REPEATABLE READ",
			"serializable":     "SERIALIZABLE",
		}
		cmd = "SET TRANSACTION ISOLATION LEVEL " + levels[level]
	case "postgres":
		levels = map[string]string{
			"read-committed":  "READ COMMITTED",
			"repeatable-read": "REPEATABLE READ",
			"serializable":    "SERIALIZABLE",
		}
		cmd = "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + levels[level]
	case "sqlite3":
		levels = map[string]string{"serializable": ""}
		cmd = "SELECT 1"
	}

	if _, ok := levels[level]; !ok {
		log.Fatalf("unknown log level %s for database %s", level, driver)
	}
	return cmd
}

// --- logger -----------------------------------------------------------------

type logger struct {
	mu sync.Mutex
	f  *os.File
}

func newLogger(path string) (*logger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &logger{f: f}, nil
}

func (l *logger) write(event map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
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
	40, // command
	14, // start
	14, // end
	20, // results
	40, // error
	40, // notes
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

func (sc *screen) pollEvents(cancel context.CancelFunc) {
	for {
		ev := sc.s.PollEvent()
		switch ev := ev.(type) {
		case *tcell.EventKey:
			switch ev.Key() {
			case tcell.KeyUp:
				sc.scroll(-1)
			case tcell.KeyDown:
				sc.scroll(1)
			default:
				cancel()
				return
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

func (sc *screen) drawLogicalRow(y int, r rowData) int {
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

	for line := 0; line < numLines; line++ {
		sy := y + line
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
	}
	return numLines
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
		screenY := headerLines + startLine
		if screenY >= h {
			break
		}

		drawn := sc.drawLogicalRow(screenY, r)
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
	_, h2 := sc.s.Size()
	if sc.scrollOffset+h2-headerLines < total {
		hint := " ↓ more "
		for i, ch := range []rune(hint) {
			sc.s.SetContent(i, h2-1, ch, nil, tcell.StyleDefault.Dim(true))
		}
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
		if len(cols) > 0 {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			for rows.Next() {
				rows.Scan(ptrs...)
				row := make([]any, len(vals))
				copy(row, vals)
				resultParts = append(resultParts, fmt.Sprintf("%v", row))
				resultRows = append(resultRows, row)
			}
		}
		if err := rows.Close(); err != nil {
			log.Fatalf("unexpected error on close [%s]: %s", id, err)
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

func main() {
	interval := flag.Duration("interval", 3*time.Second, "delay between dispatching each step (e.g. 500ms, 1s, 1m)")
	logPath := flag.String("log", "monastery.jsonl", "path to JSON log file")
	flag.Parse()

	args := flag.Args()
	if len(args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: monastery [-interval <duration>] [-log <path>] <driver> <dsn> <isolation level> <script>")
		fmt.Fprintln(os.Stderr, "  -interval duration  delay between steps (default 3s, e.g. 500ms, 1m)")
		fmt.Fprintln(os.Stderr, "  -log path           json log file (default monastery.jsonl)")
		fmt.Fprintln(os.Stderr, "  isolation levels: read-uncommitted, read-committed, repeatable-read, serializable")
		fmt.Fprintln(os.Stderr, "  drivers: mysql, postgres, sqlite3")
		fmt.Fprintln(os.Stderr, "  mysql:    root:pass@tcp(127.0.0.1:3306)/dbname")
		fmt.Fprintln(os.Stderr, "  postgres: host=localhost user=postgres dbname=test sslmode=disable")
		fmt.Fprintln(os.Stderr, "  sqlite3:  ./test.db")
		os.Exit(1)
	}

	driver, dsn, isolationLevel, scriptPath := args[0], args[1], args[2], args[3]

	f, err := os.Open(scriptPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open script:", err)
		os.Exit(1)
	}
	defer f.Close()

	steps, err := parseScript(f)
	if err != nil {
		fmt.Fprintln(os.Stderr, "parse script:", err)
		os.Exit(1)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open db:", err)
		os.Exit(1)
	}
	defer db.Close()

	lg, err := newLogger(*logPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open log:", err)
		os.Exit(1)
	}
	defer lg.close()

	lg.write(map[string]any{
		"event":           "session_start",
		"time":            time.Now().Format(time.RFC3339Nano),
		"driver":          driver,
		"isolation_level": isolationLevel,
		"script":          scriptPath,
		"interval_ms":     interval.Milliseconds(),
	})

	sc, err := newScreen()
	if err != nil {
		log.Fatal(err)
	}
	defer sc.fini()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go sc.pollEvents(cancel)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	setSQL := isolationSQL(driver, isolationLevel)

	clientChans := map[string]chan step{}
	var wg sync.WaitGroup

	for _, s := range steps {
		if _, ok := clientChans[s.clientID]; !ok {
			conn, err := db.Conn(ctx)
			if err != nil {
				sc.fini()
				fmt.Fprintln(os.Stderr, "get conn:", err)
				os.Exit(1)
			}
			if _, err := conn.ExecContext(ctx, setSQL); err != nil {
				sc.fini()
				log.Fatalf("set isolation level on %s: %v", s.clientID, err)
			}

			ch := make(chan step, len(steps))
			clientChans[s.clientID] = ch

			wg.Add(1)
			go func(id string, c *sql.Conn, ch chan step) {
				defer wg.Done()
				defer c.Close()
				clientWork(ctx, id, c, ch, sc, lg)
			}(s.clientID, conn, ch)
		}
	}

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

	for _, ch := range clientChans {
		close(ch)
	}
	wg.Wait()
	lg.write(map[string]any{
		"event": "session_end",
		"time":  time.Now().Format(time.RFC3339Nano),
	})
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
