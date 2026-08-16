package main

import (
	"os"
	"strings"
	"testing"
)

func parseString(t *testing.T, src string) ([]string, []step, []step, bool) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "script*.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(src); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Seek(0, 0); err != nil {
		t.Fatal(err)
	}
	pre, steps, checks, concurrent, err := parseScript(f)
	if err != nil {
		t.Fatalf("parseScript: %v", err)
	}
	return pre, steps, checks, concurrent
}

func TestSplitBind(t *testing.T) {
	cases := []struct {
		in, binds, rest string
	}{
		{"a = select balance from t", "a", "select balance from t"},
		{"a, b = select balance, balance - 1 from t", "a,b", "select balance, balance - 1 from t"},
		{"select balance from t", "", "select balance from t"},
		{"update t set balance = 5 where id = 1", "", "update t set balance = 5 where id = 1"},
		{"begin batch update t set v = 1; apply batch", "", "begin batch update t set v = 1; apply batch"},
	}
	for _, c := range cases {
		binds, rest := splitBind(c.in)
		if strings.Join(binds, ",") != c.binds || rest != c.rest {
			t.Errorf("splitBind(%q) = (%v, %q), want (%q, %q)", c.in, binds, rest, c.binds, c.rest)
		}
	}
}

func TestExpandBinds(t *testing.T) {
	binds := map[string]string{"a": "90", "b": "110"}
	got := expandBinds("update t set v = {a}, w = {b}, s = s + {42}", binds)
	want := "update t set v = 90, w = 110, s = s + {42}"
	if got != want {
		t.Errorf("expandBinds = %q, want %q", got, want)
	}
}

func TestThreeSectionsAndConcurrent(t *testing.T) {
	pre, steps, checks, concurrent := parseString(t, `
create table t (id int primary key, v int);
--- concurrent
t1: a = select v from t where id = 1;
t1: update t set v = {a} + 1 where id = 1;
---
check: select sum(v) from t; -- assert ({200})
`)
	if len(pre) != 1 {
		t.Errorf("preconditions = %d, want 1", len(pre))
	}
	if !concurrent {
		t.Error("concurrent = false, want true")
	}
	if len(steps) != 2 {
		t.Fatalf("steps = %d, want 2", len(steps))
	}
	if strings.Join(steps[0].binds, ",") != "a" {
		t.Errorf("steps[0].binds = %v, want [a]", steps[0].binds)
	}
	if steps[1].sql != "update t set v = {a} + 1 where id = 1;" {
		t.Errorf("steps[1].sql = %q", steps[1].sql)
	}
	if len(checks) != 1 {
		t.Fatalf("checks = %d, want 1", len(checks))
	}
	if len(checks[0].assert.alts) == 0 {
		t.Error("check lost its assertion")
	}
}

func TestRepeatUnrolls(t *testing.T) {
	_, steps, _, _ := parseString(t, `
create table t (id int primary key, v int);
--- concurrent
t1: repeat 3 {
  a = select v from t where id = 1;
  update t set v = {a} + 1 where id = 1;
}
t2: select v from t where id = 1;
`)
	if len(steps) != 7 {
		t.Fatalf("steps = %d, want 7 (3*2 + 1)", len(steps))
	}
	for i := 0; i < 6; i++ {
		if steps[i].clientID != "t1" {
			t.Errorf("steps[%d].clientID = %q, want t1", i, steps[i].clientID)
		}
	}
	for _, i := range []int{0, 2, 4} {
		if strings.Join(steps[i].binds, ",") != "a" {
			t.Errorf("bind lost at steps[%d] during repeat expansion", i)
		}
	}
	if steps[6].clientID != "t2" {
		t.Errorf("steps[6].clientID = %q, want t2", steps[6].clientID)
	}
}

// A retry block stays one step: unlike repeat, how many times it runs is
// decided at run time by whether its members succeeded.
func TestRetryStaysOneStep(t *testing.T) {
	_, steps, _, _ := parseString(t, `
---
t1: retry 20 {
  a = select v from t where id = 1;
  update t set v = {a} where id = 1 if v = {a}; -- assert ({true})
}
`)
	if len(steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(steps))
	}
	if steps[0].clientID != "t1" || steps[0].attempts != 20 {
		t.Errorf("step = %+v, want clientID t1 and 20 attempts", steps[0])
	}
	if len(steps[0].body) != 2 {
		t.Fatalf("body = %d steps, want 2", len(steps[0].body))
	}
	if steps[0].body[0].clientID != "t1" {
		t.Errorf("body[0].clientID = %q, want t1", steps[0].body[0].clientID)
	}
	if len(steps[0].body[1].assert.alts) == 0 {
		t.Error("body step lost its assertion")
	}
}

// The loop variable is substituted at parse time, so a retry inside the
// block re-runs the same statement text, including the assert, rather
// than drawing a fresh number.
func TestRepeatLoopVariable(t *testing.T) {
	_, steps, _, _ := parseString(t, `
---
t1: repeat 3 as op {
  retry 5 {
    update t set op1 = {op} where id = 1 if op1 < {op};
    select op1 from t where id = 1; -- assert ({{op}})
  }
}
`)
	if len(steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(steps))
	}
	for i, want := range []string{"1", "2", "3"} {
		body := steps[i].body
		if len(body) != 2 {
			t.Fatalf("steps[%d].body = %d, want 2", i, len(body))
		}
		if got := body[0].sql; got != "update t set op1 = "+want+" where id = 1 if op1 < "+want+";" {
			t.Errorf("steps[%d] statement = %q", i, got)
		}
		if got := body[1].assert.raw; got != "({"+want+"})" {
			t.Errorf("steps[%d] assert = %q, want ({%s})", i, got, want)
		}
	}
}

func TestRetryNestsInsideRepeat(t *testing.T) {
	_, steps, _, _ := parseString(t, `
---
t1: repeat 3 {
  select 1;
  retry 20 {
    a = select v from t where id = 1;
    update t set v = {a} where id = 1 if v = {a};
  }
}
`)
	if len(steps) != 6 {
		t.Fatalf("steps = %d, want 6 (3 * (1 plain + 1 retry))", len(steps))
	}
	for i := 0; i < 6; i += 2 {
		if steps[i].sql != "select 1;" {
			t.Errorf("steps[%d].sql = %q, want the plain statement", i, steps[i].sql)
		}
		if steps[i+1].attempts != 20 || len(steps[i+1].body) != 2 {
			t.Errorf("steps[%d] is not the unrolled retry block: %+v", i+1, steps[i+1])
		}
	}
}

func TestAssertSum(t *testing.T) {
	cases := []struct {
		assert string
		name   string
		rows   [][]any
		want   string
	}{
		{"sum(0) = 200", "two rows of one cell", [][]any{{int64(100)}, {int64(100)}}, "OK"},
		{"sum(0) = 200", "extra columns ignored", [][]any{{int64(50), "x"}, {int64(150), "y"}}, "OK"},
		{"sum(0) = 200", "torn", [][]any{{int64(99)}, {int64(100)}}, "FAIL"},
		{"sum(0) = 200", "text digits from a driver", [][]any{{"50"}, {"150"}}, "OK"},
		{"sum(0) = 200", "no rows", nil, "FAIL"},
		{"sum(0) = 200", "selected cell non-numeric", [][]any{{"abc", int64(200)}}, "FAIL"},
		{"sum(0, 1) = 200", "two cells in one row", [][]any{{int64(50), int64(150)}}, "OK"},
		{"sum(0, 1) = 200", "index out of range", [][]any{{int64(200)}}, "FAIL"},
		{"sum(0, 1) = 200", "multi-row multi-column", [][]any{{int64(100), int64(50)}, {int64(25), int64(25)}}, "OK"},
	}
	for _, c := range cases {
		a, err := parseAssert(c.assert)
		if err != nil {
			t.Fatalf("parseAssert(%q): %v", c.assert, err)
		}
		if got, _ := evalAssert(a, nil, c.rows); got != c.want {
			t.Errorf("%s / %s: evalAssert = %q, want %q", c.assert, c.name, got, c.want)
		}
	}
	a, err := parseAssert("sum(0) = 200")
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := evalAssert(a, os.ErrClosed, [][]any{{int64(200)}}); got != "FAIL" {
		t.Errorf("errored step: evalAssert = %q, want FAIL", got)
	}
	for _, bad := range []string{"sum = 200", "sum() = 200", "sum(x) = 200", "sum(0) = abc", "sum(0)"} {
		if _, err := parseAssert(bad); err == nil {
			t.Errorf("parseAssert(%q): want error, got nil", bad)
		}
	}
}

func TestBackwardCompatibleTwoSection(t *testing.T) {
	pre, steps, checks, concurrent := parseString(t, `
drop table if exists test;
---
t1: begin;
t1: select * from test; -- assert ({1, 10})
t1: commit;
`)
	if concurrent {
		t.Error("concurrent = true, want false")
	}
	if len(pre) != 1 || len(steps) != 3 || len(checks) != 0 {
		t.Errorf("pre=%d steps=%d checks=%d, want 1/3/0", len(pre), len(steps), len(checks))
	}
}

func TestUnclosedBlockErrors(t *testing.T) {
	for _, src := range []string{
		"---\nt1: repeat 2 {\n  select 1;\n",
		"---\nt1: retry 2 {\n  select 1;\n",
		"---\nt1: repeat 2 {\n  retry 3 {\n    select 1;\n  }\n",
	} {
		f, err := os.CreateTemp(t.TempDir(), "script*.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(src); err != nil {
			t.Fatal(err)
		}
		if _, err := f.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		if _, _, _, _, err := parseScript(f); err == nil {
			t.Errorf("want error for unclosed block in %q, got nil", src)
		}
	}
}
