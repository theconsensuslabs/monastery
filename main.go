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
	"os"
	"os/signal"
	"path/filepath"
	"plugin"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

type assertKind int

const (
	assertError assertKind = iota
	assertOK
	assertRows
)

type assertAlt struct {
	kind assertKind
	rows [][]string
}

type assertion struct {
	alts []assertAlt
	raw  string
}

type step struct {
	clientID string
	sql      string
	assert   assertion
}

// commentMarker requires surrounding whitespace so it doesn't collide with
// `--` appearing inside SQL literals or identifiers.
const commentMarker = " -- "
const assertPrefix = "assert"

// splitOr splits an assert expression on top-level ` or ` separators,
// ignoring occurrences inside `(...)` or `{...}`.
func splitOr(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); {
		switch s[i] {
		case '(', '{':
			depth++
		case ')', '}':
			depth--
		}
		if depth == 0 && i+4 <= len(s) && s[i:i+4] == " or " {
			parts = append(parts, strings.TrimSpace(s[start:i]))
			i += 4
			start = i
			continue
		}
		i++
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

// parseAssertAlt parses a single alternative: `error`, `ok`, `()`, or
// `({a, b}, {c, d}, ...)`. The outer parens denote an unordered set of
// rows; inner braces denote a row tuple.
func parseAssertAlt(s string) (assertAlt, error) {
	s = strings.TrimSpace(s)
	switch s {
	case "error":
		return assertAlt{kind: assertError}, nil
	case "ok":
		return assertAlt{kind: assertOK}, nil
	}
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		return assertAlt{}, fmt.Errorf("invalid assert %q: expected `error`, `ok`, or `({a, b}, ...)`", s)
	}
	body := strings.TrimSpace(s[1 : len(s)-1])
	a := assertAlt{kind: assertRows}
	for i := 0; i < len(body); {
		for i < len(body) && (body[i] == ' ' || body[i] == ',' || body[i] == '\t') {
			i++
		}
		if i >= len(body) {
			break
		}
		if body[i] != '{' {
			return assertAlt{}, fmt.Errorf("invalid assert %q: expected '{' at offset %d", s, i)
		}
		i++
		end := strings.IndexByte(body[i:], '}')
		if end == -1 {
			return assertAlt{}, fmt.Errorf("invalid assert %q: missing '}'", s)
		}
		var fields []string
		for _, f := range strings.Split(body[i:i+end], ",") {
			fields = append(fields, strings.TrimSpace(f))
		}
		a.rows = append(a.rows, fields)
		i += end + 1
	}
	return a, nil
}

// parseAssert parses an expression after `assert`. Alternatives may be
// chained with ` or `; the assertion holds if any alternative matches.
func parseAssert(s string) (assertion, error) {
	s = strings.TrimSpace(s)
	a := assertion{raw: s}
	for _, part := range splitOr(s) {
		alt, err := parseAssertAlt(part)
		if err != nil {
			return assertion{}, err
		}
		a.alts = append(a.alts, alt)
	}
	return a, nil
}

// parseStep returns a zero step (sql == "") to indicate the line had no
// command (blank, malformed, or comment-only).
func parseStep(line string) (step, error) {
	if idx := strings.Index(line, commentMarker); idx != -1 {
		comment := strings.TrimSpace(line[idx+len(commentMarker):])
		line = line[:idx]
		if comment == assertPrefix || strings.HasPrefix(comment, assertPrefix+" ") {
			expr := strings.TrimSpace(strings.TrimPrefix(comment, assertPrefix))
			// Free-text comment after the assertion, delimited by `#`.
			if idx := strings.Index(expr, "#"); idx != -1 {
				expr = strings.TrimSpace(expr[:idx])
			}
			a, err := parseAssert(expr)
			if err != nil {
				return step{}, err
			}
			parts := strings.SplitN(line, ":", 2)
			if len(parts) < 2 {
				return step{}, nil
			}
			query := strings.TrimSpace(parts[1])
			if query == "" {
				return step{}, nil
			}
			return step{strings.TrimSpace(parts[0]), query, a}, nil
		}
	}
	parts := strings.SplitN(line, ":", 2)
	if len(parts) < 2 {
		return step{}, nil
	}
	query := strings.TrimSpace(parts[1])
	if query == "" {
		return step{}, nil
	}
	return step{strings.TrimSpace(parts[0]), query, assertion{}}, nil
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
		s, err := parseStep(line)
		if err != nil {
			return nil, nil, err
		}
		if s.sql != "" {
			steps = append(steps, s)
		}
	}
	return pre, steps, nil
}

// evalAssertAlt returns true if a single alternative is satisfied.
// Row comparison is order-insensitive (multiset): SQL engines don't
// guarantee row order without ORDER BY.
func evalAssertAlt(a assertAlt, runErr error, rows [][]any) bool {
	switch a.kind {
	case assertError:
		return runErr != nil
	case assertOK:
		return runErr == nil
	case assertRows:
		if runErr != nil || len(rows) != len(a.rows) {
			return false
		}
		used := make([]bool, len(rows))
		for _, want := range a.rows {
			matched := false
			for j, got := range rows {
				if used[j] || !rowEqual(got, want) {
					continue
				}
				used[j] = true
				matched = true
				break
			}
			if !matched {
				return false
			}
		}
		return true
	}
	return false
}

func rowEqual(got []any, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i, v := range got {
		if formatValue(v) != want[i] {
			return false
		}
	}
	return true
}

// evalAssert returns "OK", "FAIL", or "" for no assertion. With multiple
// alternatives, the assertion holds if any matches.
func evalAssert(a assertion, runErr error, rows [][]any) string {
	if len(a.alts) == 0 {
		return ""
	}
	for _, alt := range a.alts {
		if evalAssertAlt(alt, runErr, rows) {
			return "OK"
		}
	}
	return "FAIL"
}

// --- logger -----------------------------------------------------------------

type logger struct {
	mu     sync.Mutex
	f      *os.File
	stdout bool
	runID  string
}

func newLogger(path string, runID string, stdout bool) (*logger, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &logger{f: f, stdout: stdout, runID: runID}, nil
}

func (l *logger) write(event map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	event["run_id"] = l.runID
	b, _ := json.Marshal(event)
	l.f.Write(b)
	l.f.Write([]byte("\n"))
	if l.stdout {
		os.Stdout.Write(b)
		os.Stdout.Write([]byte("\n"))
	}
}

func (l *logger) close() {
	l.f.Close()
}

// --- worker -----------------------------------------------------------------

// formatValue renders a single column value into a stable string that
// doesn't depend on the driver's choice of Go type. Drivers vary in what
// they hand back for the same logical column (e.g. mysql returns []byte
// for ints by default, pgx returns int64), so we normalize here.
func formatValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(x)
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int:
		return strconv.Itoa(x)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func formatRow(row []any) string {
	parts := make([]string, len(row))
	for i, v := range row {
		parts[i] = formatValue(v)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// formatRows renders the actual result set in the same syntax that
// `-- assert ...` uses, so RESULTS and ASSERT are visually aligned.
func formatRows(rows [][]any) string {
	parts := make([]string, len(rows))
	for i, r := range rows {
		parts[i] = formatRow(r)
	}
	return "(" + strings.Join(parts, ", ") + ")"
}

func clientWork(ctx context.Context, id string, conn *sql.Conn, ch <-chan step, sc *screen, lg *logger, failures *atomic.Int64) {
	for s := range ch {
		start := time.Now()

		lg.write(map[string]any{
			"event":   "query_start",
			"client":  id,
			"command": s.sql,
			"assert":  s.assert.raw,
			"time":    start.Format(time.RFC3339Nano),
		})

		idx := sc.addRow(rowData{
			client:  id,
			command: s.sql,
			start:   start.Format("15:04:05.000"),
			end:     "pending",
			assert:  s.assert.raw,
		})

		var (
			cols       []string
			resultRows [][]any
			runErr     error
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
			"assert":     s.assert.raw,
			"start_time": start.Format(time.RFC3339Nano),
			"end_time":   end.Format(time.RFC3339Nano),
			"elapsed_ms": end.Sub(start).Milliseconds(),
		}
		row := rowData{
			client:  id,
			command: s.sql,
			start:   start.Format("15:04:05.000"),
			end:     end.Format("15:04:05.000"),
			assert:  s.assert.raw,
		}
		if runErr != nil {
			event["error"] = runErr.Error()
			row.err = runErr.Error()
		} else {
			event["columns"] = cols
			event["rows"] = resultRows
			event["row_count"] = len(resultRows)
			row.results = formatRows(resultRows)
		}
		status := evalAssert(s.assert, runErr, resultRows)
		row.assertStatus = status
		if status != "" {
			event["assert_status"] = status
			if status == "FAIL" {
				failures.Add(1)
			}
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

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	interval := flag.Duration("interval", 300*time.Millisecond, "delay between dispatching each step (e.g. 500ms, 1s, 1m)")
	interactive := flag.Bool("interactive", false, "wait for any keypress before dispatching each step")
	eventsOnly := flag.Bool("events-only", false, "skip the TUI and stream JSON events to stdout")
	logPath := flag.String("log", "monastery.jsonl", "path to JSON log file")
	pluginDir := flag.String("plugin-dir", defaultPluginDir(), "directory containing driver plugin .so files")
	flag.Parse()

	args := flag.Args()
	if len(args) < 4 {
		return errors.New("usage: monastery [-interval <duration>] [-interactive] [-events-only] [-log <path>] <driver> <dsn> <isolation level> <script>\n" +
			"  -interval duration  delay between steps (default 3s, e.g. 500ms, 1m)\n" +
			"  -interactive        wait for keypress before each step instead of using interval\n" +
			"  -events-only        skip the TUI and stream JSON events to stdout\n" +
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

	lg, err := newLogger(*logPath, runID, *eventsOnly)
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

	var sc *screen
	if !*eventsOnly {
		sc, err = newScreen()
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
	}

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

	var nextStep chan struct{}
	if *interactive {
		nextStep = make(chan struct{}, 1)
	}
	if sc != nil {
		// Wake pollEvents on shutdown so it can exit cleanly instead of
		// blocking on a finalized screen.
		go func() {
			<-ctx.Done()
			sc.s.PostEvent(tcell.NewEventInterrupt(nil))
		}()
		go sc.pollEvents(cancel, nextStep)
	}

	if err := runPreconditions(ctx, db, sc, lg, driver, preconditions); err != nil {
		return err
	}

	var failures atomic.Int64
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
				clientWork(ctx, id, c, ch, sc, lg, &failures)
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

	if *interactive {
		dispatch(func() bool {
			select {
			case <-ctx.Done():
				return false
			case <-nextStep:
				return true
			}
		})
	} else {
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

	failed := failures.Load()
	sessionEnd := map[string]any{
		"event":           "session_end",
		"time":            time.Now().Format(time.RFC3339Nano),
		"assert_failures": failed,
	}
	if interrupted {
		sessionEnd["reason"] = "interrupted"
	}
	lg.write(sessionEnd)

	failErr := func() error {
		if failed > 0 {
			return fmt.Errorf("%d assertion(s) failed", failed)
		}
		return nil
	}

	// Skip the post-run pause when there's no TUI, nothing watching (pipe),
	// or shutdown was already triggered.
	if interrupted || sc == nil || !isTerminal(os.Stdout) {
		return failErr()
	}
	sc.mu.Lock()
	sc.allDone = true
	sc.redrawAll()
	sc.mu.Unlock()
	<-ctx.Done()
	return failErr()
}
