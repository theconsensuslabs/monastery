package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
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

// notesMarker requires surrounding whitespace so it doesn't collide with
// `--` appearing inside SQL literals or identifiers.
const notesMarker = " -- "

func parseStep(line string) (step, bool) {
	var notes string
	if idx := strings.Index(line, notesMarker); idx != -1 {
		notes = strings.TrimSpace(line[idx+len(notesMarker):])
		line = line[:idx]
	}
	parts := strings.SplitN(line, ":", 2)
	if len(parts) < 2 {
		return step{}, false
	}
	query := strings.TrimSpace(parts[1])
	if query == "" {
		return step{}, false
	}
	return step{strings.TrimSpace(parts[0]), query, notes}, true
}

func parseScript(f *os.File) (preconditions []string, steps []step, err error) {
	scanner := bufio.NewScanner(f)
	var pre, post []string
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
		if seenSeparator {
			post = append(post, line)
		} else {
			pre = append(pre, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	// no separator: everything was steps, not preconditions
	if !seenSeparator {
		post, pre = pre, nil
	}
	for _, line := range post {
		if s, ok := parseStep(line); ok {
			steps = append(steps, s)
		}
	}
	return pre, steps, nil
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

var colDefaultWidths = [numCols]int{
	10, // client
	60, // command
	14, // start
	14, // end
	20, // results
	40, // error
	50, // notes
}

// Minimum widths preserve readability of fixed-format fields
// (timestamps need 12 for HH:MM:SS.mmm) while letting wide free-text
// columns shrink down further.
var colMinWidths = [numCols]int{
	4,  // client
	10, // command
	12, // start
	12, // end
	8,  // results
	10, // error
	10, // notes
}

var colWidths = colDefaultWidths
var colOffsets [numCols]int

var colHeaders = [numCols]string{
	"CLIENT", "COMMAND", "STARTED", "ENDED", "RESULTS", "ERROR", "NOTES",
}

func init() {
	recomputeColOffsets()
}

func recomputeColOffsets() {
	x := 1
	for i, w := range colWidths {
		colOffsets[i] = x
		x += w + 1
	}
}

// recomputeColWidths fits the column layout to the current screen width.
// numCols+1 cells go to borders/separators; the rest is split among columns,
// preferring defaults when there's room and otherwise distributing the
// available space above each column's minimum proportionally to its slack.
func recomputeColWidths(screenW int) {
	overhead := numCols + 1
	sumDefault := 0
	for _, d := range colDefaultWidths {
		sumDefault += d
	}
	if screenW >= sumDefault+overhead {
		colWidths = colDefaultWidths
		recomputeColOffsets()
		return
	}
	sumMin := 0
	for _, m := range colMinWidths {
		sumMin += m
	}
	avail := screenW - overhead
	if avail <= sumMin {
		// even minimums don't fit — scale them proportionally, never below 1
		for i, m := range colMinWidths {
			w := 1
			if avail > 0 {
				w = m * avail / sumMin
				if w < 1 {
					w = 1
				}
			}
			colWidths[i] = w
		}
		recomputeColOffsets()
		return
	}
	extra := avail - sumMin
	slack := sumDefault - sumMin
	distributed := 0
	for i := 0; i < numCols-1; i++ {
		add := (colDefaultWidths[i] - colMinWidths[i]) * extra / slack
		colWidths[i] = colMinWidths[i] + add
		distributed += add
	}
	colWidths[numCols-1] = colMinWidths[numCols-1] + (extra - distributed)
	recomputeColOffsets()
}

func totalWidth() int {
	w := 1
	for _, cw := range colWidths {
		w += cw + 1
	}
	return w
}

// --- text wrapping ----------------------------------------------------------

// wrapText breaks text into lines no wider than width, preferring whitespace
// boundaries. Falls back to a hard wrap when no space fits.
func wrapText(text string, width int) []string {
	if width <= 0 {
		return []string{text}
	}
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{""}
	}
	var lines []string
	for len(runes) > width {
		breakAt := -1
		for i := width; i > 0; i-- {
			if runes[i] == ' ' {
				breakAt = i
				break
			}
		}
		if breakAt > 0 {
			lines = append(lines, string(runes[:breakAt]))
			runes = runes[breakAt+1:]
		} else {
			lines = append(lines, string(runes[:width]))
			runes = runes[width:]
		}
	}
	lines = append(lines, string(runes))
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
	dumping      bool
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
	return strings.TrimRight(sb.String(), "\n") + "\n"
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
	w, _ := s.Size()
	recomputeColWidths(w)
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
				cancel()
				return
			default:
				if ev.Rune() == 'q' || ev.Rune() == 'Q' {
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
		case *tcell.EventInterrupt:
			return
		}
	}
}

// contentBounds returns the y-range available for rows. The status bar
// occupies y=h-1 in normal mode; when content overflows, an extra row is
// reserved at the top (for ↑ more) and bottom (for ↓ more). Dumping mode
// uses the entire screen so the saved table has no spurious blank rows.
func contentBounds(h, total int, dumping bool) (top, bottom, visible int) {
	top = headerLines
	if dumping {
		bottom = h
	} else {
		bottom = h - 1
	}
	visible = bottom - top
	if !dumping && total > visible {
		top++
		bottom--
		visible = bottom - top
	}
	if visible < 0 {
		visible = 0
	}
	return
}

func (sc *screen) scroll(delta int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	_, h := sc.s.Size()
	total := sc.totalContentLines()
	_, _, visible := contentBounds(h, total, false)
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
		endStyle, defaultStyle, errStyle, defaultStyle,
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
	w, h := sc.s.Size()
	recomputeColWidths(w)
	sc.s.Clear()
	sc.drawHeader()
	total := sc.totalContentLines()
	contentTop, contentBottom, visible := contentBounds(h, total, sc.dumping)

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
		screenY := contentTop + startLine
		if screenY >= contentBottom {
			break
		}

		drawn := sc.drawLogicalRow(screenY, r, firstLine)
		divY := screenY + drawn
		if divY < contentBottom {
			if i < len(sc.rows)-1 {
				sc.drawHRule(divY)
			} else {
				sc.drawBorder(divY, '└', '┴', '┘')
			}
		}

		skipped += rowScreenLines
	}

	if sc.dumping {
		sc.s.Show()
		return
	}

	dim := tcell.StyleDefault.Dim(true)
	if sc.scrollOffset > 0 {
		for i, ch := range []rune(" ↑ more ") {
			sc.s.SetContent(i, headerLines, ch, nil, dim)
		}
	}
	if sc.scrollOffset+visible < total {
		for i, ch := range []rune(" ↓ more ") {
			sc.s.SetContent(i, h-2, ch, nil, dim)
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
	for i, ch := range hintRunes {
		sc.s.SetContent(startX+i, h-1, ch, nil, dim)
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

func formatValue(v any) string {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}

func formatResultRow(row []any) string {
	parts := make([]string, len(row))
	for i, v := range row {
		parts[i] = formatValue(v)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

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

		var (
			cols        []string
			resultParts []string
			resultRows  [][]any
			runErr      error
		)

		rows, err := conn.QueryContext(ctx, s.sql)
		if err != nil {
			runErr = err
		} else {
			cols, runErr = rows.Columns()
			if runErr == nil && len(cols) > 0 {
				vals := make([]any, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				for rows.Next() {
					if err := rows.Scan(ptrs...); err != nil {
						runErr = err
						break
					}
					row := make([]any, len(vals))
					copy(row, vals)
					resultParts = append(resultParts, formatResultRow(row))
					resultRows = append(resultRows, row)
				}
			}
			if runErr == nil {
				runErr = rows.Err()
			}
			if err := rows.Close(); err != nil && runErr == nil {
				runErr = err
			}
		}

		end := time.Now()
		event := map[string]any{
			"event":      "query_end",
			"client":     id,
			"command":    s.sql,
			"notes":      s.notes,
			"start_time": start.Format(time.RFC3339Nano),
			"end_time":   end.Format(time.RFC3339Nano),
			"elapsed_ms": end.Sub(start).Milliseconds(),
		}
		row := rowData{
			client:  id,
			command: s.sql,
			start:   start.Format("15:04:05.000"),
			end:     end.Format("15:04:05.000"),
			notes:   s.notes,
		}
		if runErr != nil {
			event["error"] = runErr.Error()
			row.err = runErr.Error()
		} else {
			event["columns"] = cols
			event["rows"] = resultRows
			event["row_count"] = len(resultRows)
			row.results = strings.Join(resultParts, "; ")
		}
		lg.write(event)
		sc.updateRow(idx, row)
	}
}

// --- preconditions ----------------------------------------------------------

func runPreconditions(ctx context.Context, db *sql.DB, sc *screen, lg *logger, driver string, preconditions []string) error {
	if len(preconditions) == 0 {
		return nil
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("precondition conn: %w", err)
	}
	defer conn.Close()
	for _, sqlText := range preconditions {
		start := time.Now()
		lg.write(map[string]any{
			"event":   "precondition_start",
			"command": sqlText,
			"time":    start.Format(time.RFC3339Nano),
			"driver":  driver,
		})
		idx := sc.addRow(rowData{
			client:  "setup",
			command: sqlText,
			start:   start.Format("15:04:05.000"),
			end:     "pending",
		})
		_, execErr := conn.ExecContext(ctx, sqlText)
		end := time.Now()
		event := map[string]any{
			"event":      "precondition_end",
			"command":    sqlText,
			"start_time": start.Format(time.RFC3339Nano),
			"end_time":   end.Format(time.RFC3339Nano),
			"elapsed_ms": end.Sub(start).Milliseconds(),
		}
		row := rowData{
			client:  "setup",
			command: sqlText,
			start:   start.Format("15:04:05.000"),
			end:     end.Format("15:04:05.000"),
		}
		if execErr != nil {
			event["error"] = execErr.Error()
			row.err = execErr.Error()
		}
		lg.write(event)
		sc.updateRow(idx, row)
		if execErr != nil {
			return fmt.Errorf("precondition %q: %w", sqlText, execErr)
		}
	}
	return nil
}

// --- main -------------------------------------------------------------------

func defaultPluginDir() string {
	exe, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(exe)
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	interval := flag.Duration("interval", 3*time.Second, "delay between dispatching each step (e.g. 500ms, 1s, 1m)")
	interactive := flag.Bool("interactive", false, "wait for any keypress before dispatching each step")
	runFlag := flag.Bool("run", false, "dispatch all steps immediately, dump output, and exit")
	logPath := flag.String("log", "monastery.jsonl", "path to JSON log file")
	pluginDir := flag.String("plugin-dir", defaultPluginDir(), "directory containing driver plugin .so files")
	flag.Parse()

	args := flag.Args()
	if len(args) < 4 {
		return errors.New("usage: monastery [-interval <duration>] [-interactive] [-log <path>] <driver> <dsn> <isolation level> <script>\n" +
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
		return fmt.Errorf("load driver plugin %s: %w", pluginPath, err)
	}
	sym, err := p.Lookup("IsolationSQL")
	if err != nil {
		return fmt.Errorf("plugin %s missing IsolationSQL symbol", pluginPath)
	}
	isolationSQLFn, ok := sym.(func(string) string)
	if !ok {
		return fmt.Errorf("plugin %s: IsolationSQL has wrong type", pluginPath)
	}
	setSQL := isolationSQLFn(isolationLevel)
	if setSQL == "" {
		return fmt.Errorf("unknown isolation level %q for driver %s", isolationLevel, driver)
	}

	showSym, err := p.Lookup("ShowIsolationSQL")
	if err != nil {
		return fmt.Errorf("plugin %s missing ShowIsolationSQL symbol", pluginPath)
	}
	showIsolationSQLFn, ok := showSym.(func() string)
	if !ok {
		return fmt.Errorf("plugin %s: ShowIsolationSQL has wrong type", pluginPath)
	}
	showSQL := showIsolationSQLFn()

	f, err := os.Open(scriptPath)
	if err != nil {
		return fmt.Errorf("open script: %w", err)
	}
	defer f.Close()

	preconditions, steps, err := parseScript(f)
	if err != nil {
		return fmt.Errorf("parse script: %w", err)
	}

	for i, p := range preconditions {
		preconditions[i] = strings.ReplaceAll(p, "$SHOW_ISOLATION", showSQL)
	}
	for i := range steps {
		steps[i].sql = strings.ReplaceAll(steps[i].sql, "$SHOW_ISOLATION", showSQL)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	runID := newUUID()

	lg, err := newLogger(*logPath, runID)
	if err != nil {
		return fmt.Errorf("open log: %w", err)
	}
	defer lg.close()

	lg.write(map[string]any{
		"event":           "session_start",
		"time":            time.Now().Format(time.RFC3339Nano),
		"driver":          driver,
		"isolation_level": isolationLevel,
		"interval_ms":     interval.Milliseconds(),
	})

	sc, err := newScreen()
	if err != nil {
		return err
	}
	defer func() {
		sc.mu.Lock()
		if sc.dump == "" {
			sc.dumping = true
			sc.redrawAll()
			sc.dump = sc.captureDump()
		}
		dump := sc.dump
		sc.mu.Unlock()
		sc.fini()
		if dump != "" {
			fmt.Print(dump)
		}
		fmt.Println(runID)
	}()
	sc.interactive = *interactive

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		select {
		case <-sigs:
			cancel()
		case <-ctx.Done():
		}
	}()

	// Wake pollEvents on shutdown so it can exit cleanly instead of
	// blocking on a finalized screen.
	go func() {
		<-ctx.Done()
		sc.s.PostEvent(tcell.NewEventInterrupt(nil))
	}()

	var nextStep chan struct{}
	if *interactive {
		nextStep = make(chan struct{}, 1)
	}
	if !*runFlag {
		go sc.pollEvents(cancel, nextStep)
	}

	if err := runPreconditions(ctx, db, sc, lg, driver, preconditions); err != nil {
		return err
	}

	clientChans := map[string]chan step{}
	var wg sync.WaitGroup

	var workerConnsMu sync.Mutex
	var workerConns []*sql.Conn
	// On cancel, close worker connections to unblock any in-flight queries
	// that don't honor ctx mid-read. Workers translate the resulting error
	// into a row-level error rather than crashing.
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
			conn, err := db.Conn(ctx)
			if err != nil {
				return fmt.Errorf("get conn for %s: %w", s.clientID, err)
			}
			if _, err := conn.ExecContext(ctx, setSQL); err != nil {
				conn.Close()
				return fmt.Errorf("set isolation level on %s: %w", s.clientID, err)
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

	interrupted := false
	dispatch := func(wait func() bool) {
		for _, s := range steps {
			select {
			case clientChans[s.clientID] <- s:
			case <-ctx.Done():
				interrupted = true
				return
			}
			if !wait() {
				interrupted = true
				return
			}
		}
	}

	switch {
	case *runFlag:
		dispatch(func() bool { return true })
	case *interactive:
		dispatch(func() bool {
			select {
			case <-ctx.Done():
				return false
			case <-nextStep:
				return true
			}
		})
	default:
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		dispatch(func() bool {
			select {
			case <-ctx.Done():
				return false
			case <-ticker.C:
				return true
			}
		})
	}

	for _, ch := range clientChans {
		close(ch)
	}
	wg.Wait()

	sessionEnd := map[string]any{
		"event": "session_end",
		"time":  time.Now().Format(time.RFC3339Nano),
	}
	if interrupted {
		sessionEnd["reason"] = "interrupted"
	}
	lg.write(sessionEnd)

	// Skip the post-run pause when there's nothing watching (pipe), when
	// the user asked for a one-shot, or when shutdown was already triggered.
	if interrupted || *runFlag || !isTerminal(os.Stdout) {
		return nil
	}
	sc.mu.Lock()
	sc.allDone = true
	sc.redrawAll()
	sc.mu.Unlock()
	<-ctx.Done()
	return nil
}
