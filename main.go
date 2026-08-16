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
	"sort"
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
	assertSum
)

type assertAlt struct {
	kind    assertKind
	rows    [][]string
	sum     float64
	sumCols []int  // column indexes summed by `sum(0, 1) = N`
	label   string // schedule label, empty = unlabeled (wildcard for group consistency)
}

type assertion struct {
	alts []assertAlt
	raw  string
}

// wildcardLabel is the synthetic label for unlabeled branches: matched
// unlabeled branches are compatible with any schedule.
const wildcardLabel = "*"

type step struct {
	clientID string
	sql      string
	assert   assertion
	group    string
	// binds names variables holding the cells of this step's first result
	// row, positionally. Later steps on the same client interpolate them
	// as {name}.
	binds []string
	// body and attempts describe a `retry N { ... }` block: run body in
	// order, and if a member errors or fails its assert, run it all again.
	body     []step
	attempts int
}

// commentMarker requires surrounding whitespace so it doesn't collide with
// `--` appearing inside SQL literals or identifiers.
const commentMarker = " -- "
const assertPrefix = "assert"
const groupPrefix = "group"

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

// isAssertLabel returns true for a simple identifier (letters, digits,
// underscore; first char non-digit). Labels guard the `=>` form so SQL-ish
// expressions don't get misread as labels.
func isAssertLabel(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		isAlpha := r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
		isDigit := r >= '0' && r <= '9'
		if i == 0 && !isAlpha {
			return false
		}
		if !isAlpha && !isDigit {
			return false
		}
	}
	return true
}

// parseAssertAlt parses a single alternative: `error`, `ok`, `()`, or
// `({a, b}, {c, d}, ...)`, optionally prefixed with `<label> => `. The
// outer parens denote an unordered set of rows; inner braces denote a row
// tuple. Labels are used to enforce per-schedule consistency across the
// members of a group (see evalScheduleGroup).
func parseAssertAlt(s string) (assertAlt, error) {
	s = strings.TrimSpace(s)
	var label string
	if idx := strings.Index(s, "=>"); idx != -1 {
		cand := strings.TrimSpace(s[:idx])
		if isAssertLabel(cand) {
			label = cand
			s = strings.TrimSpace(s[idx+2:])
		}
	}
	switch s {
	case "error":
		return assertAlt{kind: assertError, label: label}, nil
	case "ok":
		return assertAlt{kind: assertOK, label: label}, nil
	}
	if rest, found := strings.CutPrefix(s, "sum("); found {
		list, after, closed := strings.Cut(rest, ")")
		val, hasEq := strings.CutPrefix(strings.TrimSpace(after), "=")
		if !closed || !hasEq {
			return assertAlt{}, fmt.Errorf("invalid assert %q: expected `sum(<col>, ...) = N`", s)
		}
		var cols []int
		for _, f := range strings.Split(list, ",") {
			i, err := strconv.Atoi(strings.TrimSpace(f))
			if err != nil || i < 0 {
				return assertAlt{}, fmt.Errorf("invalid assert %q: bad column index %q", s, f)
			}
			cols = append(cols, i)
		}
		n, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
		if err != nil {
			return assertAlt{}, fmt.Errorf("invalid assert %q: sum needs a number", s)
		}
		return assertAlt{kind: assertSum, sum: n, sumCols: cols, label: label}, nil
	}
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		return assertAlt{}, fmt.Errorf("invalid assert %q: expected `error`, `ok`, `sum(<col>, ...) = N`, or `({a, b}, ...)`", s)
	}
	body := strings.TrimSpace(s[1 : len(s)-1])
	a := assertAlt{kind: assertRows, label: label}
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

// parseDirectives parses the comment portion after ` -- ` as a `;`-separated
// list of directives. Free text after `#` is dropped. Recognized directives
// are `assert <expr>` and `group <name>`; both are optional and composable.
func parseDirectives(comment string) (assertion, string, error) {
	if idx := strings.Index(comment, "#"); idx != -1 {
		comment = strings.TrimSpace(comment[:idx])
	}
	var a assertion
	var group string
	for _, raw := range strings.Split(comment, ";") {
		d := strings.TrimSpace(raw)
		if d == "" {
			continue
		}
		switch {
		case d == assertPrefix || strings.HasPrefix(d, assertPrefix+" "):
			expr := strings.TrimSpace(strings.TrimPrefix(d, assertPrefix))
			parsed, err := parseAssert(expr)
			if err != nil {
				return assertion{}, "", err
			}
			a = parsed
		case strings.HasPrefix(d, groupPrefix+" "):
			name := strings.TrimSpace(strings.TrimPrefix(d, groupPrefix))
			if name == "" {
				return assertion{}, "", fmt.Errorf("invalid group directive: missing name")
			}
			group = name
		default:
			return assertion{}, "", fmt.Errorf("unknown directive %q (expected `assert ...` or `group <name>`)", d)
		}
	}
	return a, group, nil
}

// parseStep returns a zero step (sql == "") to indicate the line had no
// command (blank, malformed, or comment-only).
func parseStep(line string) (step, error) {
	var a assertion
	var group string
	if idx := strings.Index(line, commentMarker); idx != -1 {
		comment := strings.TrimSpace(line[idx+len(commentMarker):])
		line = line[:idx]
		var err error
		a, group, err = parseDirectives(comment)
		if err != nil {
			return step{}, err
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
	binds, query := splitBind(query)
	return step{clientID: strings.TrimSpace(parts[0]), sql: query, assert: a, group: group, binds: binds}, nil
}

// splitBind recognises `name = <statement>` and `name, other = <statement>`,
// returning the variable names plus the statement. Every left-side element
// must be a bare identifier. Anything else is a statement such as
// `update ... set col = ...`.
func splitBind(query string) (binds []string, rest string) {
	idx := strings.Index(query, "=")
	if idx == -1 {
		return nil, query
	}
	remainder := strings.TrimSpace(query[idx+1:])
	if remainder == "" {
		return nil, query
	}
	for _, name := range strings.Split(query[:idx], ",") {
		name = strings.TrimSpace(name)
		if name == "" || !isIdentifier(name) {
			return nil, query
		}
		binds = append(binds, name)
	}
	return binds, remainder
}

func isIdentifier(s string) bool {
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// parseScript reads up to three sections separated by `---`:
// preconditions, steps, and post-run checks. Marking the first separator
// `--- concurrent` lets each client run its own steps at full speed instead
// of the file specifying one global interleaving.
func parseScript(f *os.File) (preconditions []string, steps, checks []step, concurrent bool, err error) {
	scanner := bufio.NewScanner(f)
	var pre, body, post []string
	section := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "---") {
			if section == 0 && strings.TrimSpace(strings.TrimPrefix(line, "---")) == "concurrent" {
				concurrent = true
			}
			section++
			continue
		}
		switch section {
		case 0:
			pre = append(pre, line)
		case 1:
			body = append(body, line)
		default:
			post = append(post, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, nil, false, err
	}
	// no separator: everything was steps, not preconditions
	if section == 0 {
		body, pre = pre, nil
	}
	if steps, err = expandSteps(body); err != nil {
		return nil, nil, nil, false, err
	}
	if checks, err = expandSteps(post); err != nil {
		return nil, nil, nil, false, err
	}
	return pre, steps, checks, concurrent, nil
}

// expandSteps parses lines into steps, expanding block forms:
// `repeat N { ... }` unrolls into N copies and `retry N { ... }` becomes one
// step the client re-runs on failure. Blocks nest.
func expandSteps(lines []string) ([]step, error) {
	return expandBlock(lines, "")
}

// expandBlock parses lines that belong to one nesting level. Lines inside a
// block carry no label of their own, so the enclosing block's is passed down.
func expandBlock(lines []string, inherited string) ([]step, error) {
	var out []step
	for i := 0; i < len(lines); i++ {
		head, isBlock := parseBlockHeader(lines[i], inherited)
		if !isBlock {
			line := lines[i]
			if inherited != "" {
				line = inherited + ": " + line
			}
			s, err := parseStep(line)
			if err != nil {
				return nil, err
			}
			if s.sql != "" {
				out = append(out, s)
			}
			continue
		}
		body, end, err := collectBlock(lines, i)
		if err != nil {
			return nil, err
		}
		i = end

		if head.keyword == "retry" {
			inner, err := expandBlock(body, head.label)
			if err != nil {
				return nil, err
			}
			out = append(out, step{clientID: head.label, body: inner, attempts: head.count})
			continue
		}
		for n := 1; n <= head.count; n++ {
			inner, err := expandBlock(substitute(body, head.loopVar, n), head.label)
			if err != nil {
				return nil, err
			}
			out = append(out, inner...)
		}
	}
	return out, nil
}

// substitute replaces {name} with the iteration number. An unnamed loop
// returns the lines unchanged.
func substitute(lines []string, name string, n int) []string {
	if name == "" {
		return lines
	}
	out := make([]string, len(lines))
	for i, line := range lines {
		out[i] = strings.ReplaceAll(line, "{"+name+"}", strconv.Itoa(n))
	}
	return out
}

// collectBlock returns the body lines of the block whose header sits at
// start, plus the index of its closing brace.
func collectBlock(lines []string, start int) (body []string, end int, err error) {
	depth := 1
	for i := start + 1; i < len(lines); i++ {
		if lines[i] == "}" {
			if depth--; depth == 0 {
				return body, i, nil
			}
		} else if _, ok := parseBlockHeader(lines[i], "_"); ok {
			depth++
		}
		body = append(body, lines[i])
	}
	return nil, 0, fmt.Errorf("block %q is not closed with }", lines[start])
}

type blockHeader struct {
	label   string
	keyword string // repeat or retry
	loopVar string // the `as n` name, empty when there is none
	count   int
}

// parseBlockHeader matches `t1: repeat 200 {` and `t1: repeat 200 as n {` at
// the top level, and the same without a label inside a block, where the label
// comes from the enclosing one.
func parseBlockHeader(line, inherited string) (blockHeader, bool) {
	label, rest := inherited, strings.TrimSpace(line)
	if inherited == "" {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			return blockHeader{}, false
		}
		label, rest = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	if !strings.HasSuffix(rest, "{") {
		return blockHeader{}, false
	}
	for _, kw := range []string{"repeat", "retry"} {
		if !strings.HasPrefix(rest, kw+" ") {
			continue
		}
		spec := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(rest, kw+" "), "{"))
		var loopVar string
		if before, after, found := strings.Cut(spec, " as "); found {
			spec, loopVar = strings.TrimSpace(before), strings.TrimSpace(after)
			if !isIdentifier(loopVar) {
				return blockHeader{}, false
			}
		}
		n, err := strconv.Atoi(spec)
		if err != nil || n <= 0 {
			return blockHeader{}, false
		}
		return blockHeader{label, kw, loopVar, n}, true
	}
	return blockHeader{}, false
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
	case assertSum:
		// A conservation invariant: the indexed cells of every row added
		// together.
		if runErr != nil || len(rows) == 0 {
			return false
		}
		var total float64
		for _, row := range rows {
			for _, i := range a.sumCols {
				if i >= len(row) {
					return false
				}
				n, ok := numericValue(row[i])
				if !ok {
					return false
				}
				total += n
			}
		}
		return total == a.sum
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

// numericValue coerces a result cell to a number, falling back to its text
// form for drivers that hand back []byte or string.
func numericValue(v any) (float64, bool) {
	switch t := v.(type) {
	case nil:
		return 0, false
	case float64:
		return t, true
	case int64:
		return float64(t), true
	}
	n, err := strconv.ParseFloat(strings.TrimSpace(formatValue(v)), 64)
	return n, err == nil
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

// evalAssert returns "OK", "FAIL", or "" for no assertion, plus the set of
// labels whose branches matched. The wildcard label "*" is added when an
// unlabeled branch matched. The label set drives schedule-group consistency
// checks (see evalScheduleGroup); it is nil when there is no assertion or
// when no branch matched.
func evalAssert(a assertion, runErr error, rows [][]any) (string, map[string]bool) {
	if len(a.alts) == 0 {
		return "", nil
	}
	matched := map[string]bool{}
	for _, alt := range a.alts {
		if !evalAssertAlt(alt, runErr, rows) {
			continue
		}
		if alt.label == "" {
			matched[wildcardLabel] = true
		} else {
			matched[alt.label] = true
		}
	}
	if len(matched) == 0 {
		return "FAIL", nil
	}
	return "OK", matched
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

// groupTracker collects per-step outcomes for steps tagged with
// `-- group <name>`. After all workers exit, evalGroups walks each named
// group in one of two modes:
//   - error mode (no member has any assertion): at least one member must
//     have errored. This is the cycle-victim invariant for engines where
//     the choice of victim is non-deterministic.
//   - schedule mode (at least one member has a labeled assertion branch):
//     there must exist some label L that every asserting member is
//     compatible with. In other words, the members' results agree on a
//     single serial schedule. See evalScheduleGroup.
type groupTracker struct {
	mu      sync.Mutex
	members map[string][]groupMember
}

type groupMember struct {
	client    string
	sql       string
	errored   bool
	hasAssert bool
	matched   map[string]bool // labels (and "*") whose branches matched
}

func newGroupTracker() *groupTracker {
	return &groupTracker{members: map[string][]groupMember{}}
}

func (g *groupTracker) record(group, client, sql string, errored, hasAssert bool, matched map[string]bool) {
	if group == "" {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.members[group] = append(g.members[group], groupMember{client, sql, errored, hasAssert, matched})
}

// groupOrder returns the unique group names in the order they first appear
// in the script. Stable iteration matters for the display table.
func groupOrder(steps []step) []string {
	seen := map[string]bool{}
	var order []string
	for _, s := range steps {
		if s.group != "" && !seen[s.group] {
			seen[s.group] = true
			order = append(order, s.group)
		}
	}
	return order
}

// groupLabels returns the set of distinct schedule labels declared across
// all asserts in members of the named group. Drives schedule-mode dispatch
// (any labels present → schedule mode) and the universe used when
// computing label feasibility.
func groupLabels(steps []step, name string) map[string]bool {
	labels := map[string]bool{}
	for _, s := range steps {
		if s.group != name {
			continue
		}
		for _, alt := range s.assert.alts {
			if alt.label != "" {
				labels[alt.label] = true
			}
		}
	}
	return labels
}

// evalGroups walks each named group and emits an OK/FAIL row for the
// invariant appropriate to the group's mode. Synthetic rows are appended
// to the display so the dump captures them.
func evalGroups(steps []step, tracker *groupTracker, sc *screen, lg *logger, failures *atomic.Int64) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	for _, name := range groupOrder(steps) {
		members := tracker.members[name]
		labels := groupLabels(steps, name)
		if len(labels) > 0 {
			evalScheduleGroup(name, members, labels, sc, lg, failures)
		} else {
			evalErrorGroup(name, members, sc, lg, failures)
		}
	}
}

func evalErrorGroup(name string, members []groupMember, sc *screen, lg *logger, failures *atomic.Int64) {
	errored := 0
	for _, m := range members {
		if m.errored {
			errored++
		}
	}
	status := "OK"
	if errored == 0 {
		status = "FAIL"
		failures.Add(1)
	}
	summary := fmt.Sprintf("%d/%d errored", errored, len(members))
	lg.write(map[string]any{
		"event":         "group_eval",
		"group":         name,
		"mode":          "error",
		"members":       len(members),
		"errored":       errored,
		"assert_status": status,
	})
	if sc != nil {
		sc.addRow(rowData{
			client:       name,
			command:      summary,
			assert:       "group: at least one error",
			assertStatus: status,
		})
	}
}

// evalScheduleGroup checks that the members' results are consistent with a
// single serial schedule: there must exist some label L in the universe
// such that every asserting member is compatible with L. A member is
// compatible with L when its matched-label set contains L or the wildcard
// (an unlabeled branch matched). Members without assertions are
// unconstrained.
func evalScheduleGroup(name string, members []groupMember, universe map[string]bool, sc *screen, lg *logger, failures *atomic.Int64) {
	feasible := map[string]bool{}
	for L := range universe {
		feasible[L] = true
	}
	for _, m := range members {
		if !m.hasAssert {
			continue
		}
		if m.matched[wildcardLabel] {
			continue
		}
		for L := range feasible {
			if !m.matched[L] {
				delete(feasible, L)
			}
		}
	}
	status := "OK"
	if len(feasible) == 0 {
		status = "FAIL"
		failures.Add(1)
	}
	feasibleList := sortedKeys(feasible)
	universeList := sortedKeys(universe)
	var summary string
	if len(feasibleList) == 0 {
		summary = "no consistent schedule"
	} else {
		summary = "schedule: {" + strings.Join(feasibleList, ", ") + "}"
	}
	lg.write(map[string]any{
		"event":         "group_eval",
		"group":         name,
		"mode":          "schedule",
		"members":       len(members),
		"universe":      universeList,
		"feasible":      feasibleList,
		"assert_status": status,
	})
	if sc != nil {
		sc.addRow(rowData{
			client:       name,
			command:      summary,
			assert:       "group: consistent schedule across {" + strings.Join(universeList, ", ") + "}",
			assertStatus: status,
		})
	}
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// txnOp classifies a SQL statement as a transaction-control op by
// prefix-match (case-insensitive). Used to track per-client txn state
// so the harness can normalize aborted-transaction semantics across
// engines that disagree (notably MariaDB, which doesn't auto-abort
// on statement errors at SERIALIZABLE).
func txnOp(sqlText string) string {
	s := strings.ToLower(strings.TrimSpace(sqlText))
	switch {
	case strings.HasPrefix(s, "begin"), strings.HasPrefix(s, "start transaction"):
		// CQL's BEGIN BATCH and BEGIN TRANSACTION carry their own terminator
		// and are single statements. A bare BEGIN opens a session
		// transaction.
		if strings.Contains(s, "apply batch") || strings.Contains(s, "commit transaction") {
			return ""
		}
		return "begin"
	case strings.HasPrefix(s, "commit"):
		return "commit"
	case strings.HasPrefix(s, "rollback"):
		return "rollback"
	}
	return ""
}

// errTxnAborted is returned for statements suppressed while the
// connection is in the harness-tracked aborted state. Mirrors
// Postgres's "current transaction is aborted, commands ignored
// until end of transaction block".
var errTxnAborted = errors.New("current transaction is aborted, statement ignored")

// expandBinds substitutes {name} with values captured by earlier steps on
// the same client. Set literals like {123} are untouched because a bind name
// must be an identifier.
func expandBinds(sqlText string, binds map[string]string) string {
	if len(binds) == 0 || !strings.Contains(sqlText, "{") {
		return sqlText
	}
	for name, val := range binds {
		sqlText = strings.ReplaceAll(sqlText, "{"+name+"}", val)
	}
	return sqlText
}

// bindLiteral renders a result cell for interpolation. Values are inserted
// bare, so a script quotes text itself: where name = '{n}'.
func bindLiteral(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case []byte:
		return string(t)
	case string:
		return t
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", t)
	}
}

func clientWork(ctx context.Context, id string, conn *sql.Conn, ch <-chan step, sc *screen, lg *logger, failures *atomic.Int64, groups *groupTracker) {
	var inTxn, txnAborted bool
	// Bindings are per-client: two clients running the same script text
	// each keep their own `a`, `b`, and so on.
	binds := map[string]string{}

	// exec runs one statement and reports whether it succeeded. attempt and
	// attempts locate it within an enclosing retry block, where only the
	// final attempt is scored.
	exec := func(s step, attempt, attempts int) bool {
		scored := attempt == attempts
		s.sql = expandBinds(s.sql, binds)
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

		op := txnOp(s.sql)

		var (
			cols       []string
			resultRows [][]any
			runErr     error
			suppressed bool
		)

		// Suppress non-BEGIN statements while the txn is in aborted
		// state. COMMIT/ROLLBACK clear the flag and report success
		// (we already issued the ROLLBACK when the abort happened).
		if txnAborted && op != "begin" {
			suppressed = true
			if op == "commit" || op == "rollback" {
				inTxn = false
				txnAborted = false
			} else {
				runErr = errTxnAborted
			}
		} else {
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

			switch {
			case op == "begin" && runErr == nil:
				inTxn = true
				txnAborted = false
			case op == "commit" || op == "rollback":
				// Txn ends regardless of whether the statement errored.
				inTxn = false
				txnAborted = false
			case runErr != nil && inTxn:
				// Engine-agnostic normalization: on any statement
				// error inside a txn, issue ROLLBACK and mark the
				// connection aborted so subsequent statements
				// short-circuit. Postgres already does this internally;
				// MySQL/MariaDB don't, so without this an errored
				// statement can leave the txn alive and produce torn
				// commits (e.g. MariaDB error 1020 at SERIALIZABLE).
				_, rbErr := conn.ExecContext(ctx, "ROLLBACK")
				inTxn = false
				txnAborted = true
				autoEvent := map[string]any{
					"event":   "auto_rollback",
					"client":  id,
					"trigger": s.sql,
					"time":    time.Now().Format(time.RFC3339Nano),
				}
				if rbErr != nil {
					autoEvent["rollback_error"] = rbErr.Error()
				}
				lg.write(autoEvent)
			}
		}

		if runErr == nil && len(resultRows) > 0 {
			for i, name := range s.binds {
				if i < len(resultRows[0]) {
					binds[name] = bindLiteral(resultRows[0][i])
				}
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
		if suppressed {
			event["suppressed"] = true
		}
		if attempts > 1 {
			event["attempt"] = attempt
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
		status, matched := evalAssert(s.assert, runErr, resultRows)
		// A losing attempt inside a retry block reports RETRY, not FAIL.
		if status == "FAIL" && !scored {
			status = "RETRY"
		}
		row.assertStatus = status
		if status != "" {
			event["assert_status"] = status
			if status == "FAIL" {
				failures.Add(1)
			}
		}
		if scored {
			groups.record(s.group, id, s.sql, runErr != nil, len(s.assert.alts) > 0, matched)
		}
		lg.write(event)
		sc.updateRow(idx, row)
		// A step's assertion, where it has one, defines success: `-- assert
		// error` succeeds when the statement was rejected.
		if len(s.assert.alts) > 0 {
			return status == "OK"
		}
		return runErr == nil
	}

	for s := range ch {
		if s.body == nil {
			exec(s, 1, 1)
			continue
		}
		for attempt := 1; attempt <= s.attempts; attempt++ {
			done := true
			for _, inner := range s.body {
				// The rest of the block is premised on this statement, so a
				// failure starts the next attempt from the top.
				if !exec(inner, attempt, s.attempts) {
					done = false
					break
				}
			}
			if done {
				break
			}
		}
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

// flagWasSet reports whether the flag was given on the command line, as
// opposed to holding its default value.
func flagWasSet(name string) bool {
	set := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
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
	markdown := flag.Bool("markdown", false, "emit the final result as a markdown table instead of the box-drawn table")
	logPath := flag.String("log", "monastery.jsonl", "path to JSON log file")
	pluginDir := flag.String("plugin-dir", defaultPluginDir(), "directory containing driver plugin .so files")
	flag.Parse()

	args := flag.Args()
	if len(args) < 3 {
		return errors.New("usage: monastery [-interval <duration>] [-interactive] [-events-only] [-markdown] [-log <path>] <driver> <dsn> [isolation level] <script>\n" +
			"  -interval duration  delay between steps (default 3s, e.g. 500ms, 1m)\n" +
			"  -interactive        wait for keypress before each step instead of using interval\n" +
			"  -events-only        skip the TUI and stream JSON events to stdout\n" +
			"  -markdown           emit the final result as a markdown table instead of the box-drawn table\n" +
			"  -log path           json log file (default monastery.jsonl)\n" +
			"  isolation levels: read-uncommitted, read-committed, repeatable-read, serializable\n" +
			"    (drivers without them, like cql, omit the argument)\n" +
			"  drivers: mysql, postgres, cql (loaded from <driver>.so plugin)\n" +
			"  mysql:    root:pass@tcp(127.0.0.1:3306)/dbname\n" +
			"  postgres: host=localhost user=postgres dbname=test sslmode=disable\n" +
			"  cql:      127.0.0.1,127.0.0.2?consistency=quorum&timeout=30s")
	}

	driver, dsn := args[0], args[1]
	var isolationLevel, scriptPath string
	if len(args) == 3 {
		scriptPath = args[2]
	} else {
		isolationLevel, scriptPath = args[2], args[3]
	}

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
		if isolationLevel == "" {
			return fmt.Errorf("driver %s requires an isolation level", driver)
		}
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

	// Per-driver overrides: foo.sql.mysql wins over foo.sql when run with
	// the mysql driver. Lets a test express engine-specific assertions
	// (e.g. accept the lock-serialized outcome on S2PL, where SI/SSI would
	// abort) without forking the whole script.
	resolvedPath := scriptPath
	if override := scriptPath + "." + driver; !strings.HasSuffix(scriptPath, "."+driver) {
		if _, statErr := os.Stat(override); statErr == nil {
			resolvedPath = override
		}
	}

	f, err := os.Open(resolvedPath)
	if err != nil {
		return fmt.Errorf("open script: %w", err)
	}
	defer f.Close()

	preconditions, steps, checks, concurrent, err := parseScript(f)
	if err != nil {
		return fmt.Errorf("parse script: %w", err)
	}

	// A concurrent run has no interleaving to watch and enough steps to
	// overwhelm the table, so it defaults to streaming events. An explicit
	// -events-only=false still gets the TUI.
	if concurrent && !flagWasSet("events-only") {
		*eventsOnly = true
	}

	for i, p := range preconditions {
		preconditions[i] = strings.ReplaceAll(p, "$SHOW_ISOLATION", showSQL)
	}
	for i := range steps {
		steps[i].sql = strings.ReplaceAll(steps[i].sql, "$SHOW_ISOLATION", showSQL)
	}
	for i := range checks {
		checks[i].sql = strings.ReplaceAll(checks[i].sql, "$SHOW_ISOLATION", showSQL)
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
		"script":          resolvedPath,
	})

	var sc *screen
	if !*eventsOnly {
		sc, err = newScreen()
		if err != nil {
			return err
		}
		defer func() {
			sc.mu.Lock()
			var output string
			if *markdown {
				output = sc.markdownDump()
			} else {
				if sc.dump == "" {
					sc.dumping = true
					sc.redrawAll()
					sc.dump = sc.captureDump()
				}
				output = sc.dump
			}
			sc.mu.Unlock()
			sc.fini()
			if output != "" {
				fmt.Print(output)
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
	groups := newGroupTracker()
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
				clientWork(ctx, id, c, ch, sc, lg, &failures, groups)
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

	if concurrent {
		// Hand every client its whole step list at once, so each runs at
		// its own pace with no globally specified interleaving. Channels
		// are buffered to len(steps), so this never blocks.
		dispatch(func() bool { return true })
	} else if *interactive {
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

	// Post-run checks: aggregate invariants over the settled state, which is
	// where assertions belong when the run had no specified interleaving.
	if !interrupted && len(checks) > 0 {
		conn, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("get conn for checks: %w", err)
		}
		workerConnsMu.Lock()
		workerConns = append(workerConns, conn)
		workerConnsMu.Unlock()

		ch := make(chan step, len(checks))
		var checkWG sync.WaitGroup
		checkWG.Add(1)
		go func() {
			defer checkWG.Done()
			clientWork(ctx, "check", conn, ch, sc, lg, &failures, groups)
		}()
		for _, s := range checks {
			ch <- s
		}
		close(ch)
		checkWG.Wait()
	}

	// Group invariants are only meaningful when every member ran. After an
	// interrupt, missing members would spuriously fail their group.
	if !interrupted {
		evalGroups(steps, groups, sc, lg, &failures)
	}

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
