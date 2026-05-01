package main

import (
	"context"
	"os"
	"strings"
	"sync"

	"github.com/gdamore/tcell/v2"
)

// --- columns ----------------------------------------------------------------

const (
	colClient = iota
	colCommand
	colStart
	colEnd
	colResults
	colError
	colAssert
	numCols
)

var colDefaultWidths = [numCols]int{
	10, // client
	60, // command
	14, // start
	14, // end
	20, // results
	40, // error
	50, // assert
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
	10, // assert
}

var colWidths = colDefaultWidths
var colOffsets [numCols]int

var colHeaders = [numCols]string{
	"CLIENT", "COMMAND", "STARTED", "ENDED", "RESULTS", "ERROR", "ASSERT",
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

func formatAssertCell(r rowData) string {
	if r.assert == "" {
		return ""
	}
	if r.assertStatus == "" {
		return r.assert
	}
	return r.assertStatus + " " + r.assert
}

func rowLines(r rowData) ([numCols][]string, int) {
	fields := [numCols]string{
		r.client, r.command, r.start, r.end, r.results, r.err, formatAssertCell(r),
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
	client       string
	command      string
	start        string
	end          string
	results      string
	err          string
	assert       string
	assertStatus string
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
	passStyle := tcell.StyleDefault.Foreground(tcell.ColorGreen)

	endStyle := defaultStyle
	if r.end == "pending" {
		endStyle = pendingStyle
	}
	errStyle := defaultStyle
	if r.err != "" {
		errStyle = errorStyle
	}
	assertStyle := defaultStyle
	switch r.assertStatus {
	case "OK":
		assertStyle = passStyle
	case "FAIL":
		assertStyle = errorStyle
	}
	styles := [numCols]tcell.Style{
		defaultStyle, defaultStyle, defaultStyle,
		endStyle, defaultStyle, errStyle, assertStyle,
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
	if sc == nil {
		return 0
	}
	sc.mu.Lock()
	defer sc.mu.Unlock()
	idx := len(sc.rows)
	sc.rows = append(sc.rows, r)
	sc.redrawAll()
	return idx
}

func (sc *screen) updateRow(idx int, r rowData) {
	if sc == nil {
		return
	}
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

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}
