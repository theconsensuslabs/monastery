package main

// This file adapts gocql to database/sql, exposing a lightweight
// transaction's [applied] column as an ordinary result row, which is
// the piece the off-the-shelf wrappers drop.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

func init() {
	sql.Register("cql", cqlDriver{})
}

// DSN form: host1,host2?consistency=quorum&timeout=30s&keyspace=lab
var consistencyNames = map[string]gocql.Consistency{
	"any":         gocql.Any,
	"one":         gocql.One,
	"two":         gocql.Two,
	"three":       gocql.Three,
	"quorum":      gocql.Quorum,
	"all":         gocql.All,
	"localquorum": gocql.LocalQuorum,
	"eachquorum":  gocql.EachQuorum,
	"localone":    gocql.LocalOne,
}

var serialNames = map[string]gocql.SerialConsistency{
	"serial":      gocql.Serial,
	"localserial": gocql.LocalSerial,
}

func parseDSN(dsn string) (*gocql.ClusterConfig, error) {
	hostPart, rawQuery := dsn, ""
	if i := strings.Index(dsn, "?"); i >= 0 {
		hostPart, rawQuery = dsn[:i], dsn[i+1:]
	}
	var hosts []string
	for _, h := range strings.Split(hostPart, ",") {
		if h = strings.TrimSpace(h); h != "" {
			hosts = append(hosts, h)
		}
	}
	if len(hosts) == 0 {
		hosts = []string{"127.0.0.1"}
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum

	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return nil, fmt.Errorf("cql: parse dsn options: %w", err)
	}
	for key, list := range values {
		value := list[len(list)-1]
		switch key {
		case "consistency":
			c, ok := consistencyNames[strings.ToLower(value)]
			if !ok {
				return nil, fmt.Errorf("cql: unknown consistency %q", value)
			}
			cluster.Consistency = c
		case "serialConsistency":
			s, ok := serialNames[strings.ToLower(value)]
			if !ok {
				return nil, fmt.Errorf("cql: unknown serial consistency %q", value)
			}
			cluster.SerialConsistency = s
		case "keyspace":
			cluster.Keyspace = value
		case "timeout", "connectTimeout":
			d, err := time.ParseDuration(value)
			if err != nil {
				return nil, fmt.Errorf("cql: %s: %w", key, err)
			}
			if key == "timeout" {
				cluster.Timeout = d
			} else {
				cluster.ConnectTimeout = d
			}
		case "protoVersion", "numConns":
			n, err := strconv.Atoi(value)
			if err != nil {
				return nil, fmt.Errorf("cql: %s: %w", key, err)
			}
			if key == "protoVersion" {
				cluster.ProtoVersion = n
			} else {
				cluster.NumConns = n
			}
		case "disableInitialHostLookup":
			cluster.DisableInitialHostLookup = value == "true"
		default:
			return nil, fmt.Errorf("cql: unknown dsn option %q", key)
		}
	}
	return cluster, nil
}

type cqlDriver struct{}

func (cqlDriver) Open(dsn string) (driver.Conn, error) {
	cluster, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return &cqlConn{session: session}, nil
}

// cqlConn holds its own gocql session and consistency settings, so each
// database/sql connection is an independent client.
type cqlConn struct {
	session     *gocql.Session
	consistency *gocql.Consistency
	serial      *gocql.SerialConsistency
}

// query builds a gocql query with this connection's consistency settings.
func (c *cqlConn) query(ctx context.Context, text string, args []driver.NamedValue) *gocql.Query {
	q := c.session.Query(text, namedToValues(args)...).WithContext(ctx)
	if c.consistency != nil {
		q = q.Consistency(*c.consistency)
	}
	if c.serial != nil {
		q = q.SerialConsistency(*c.serial)
	}
	return q
}

// setConsistency handles the client-side `CONSISTENCY <level>` and
// `SERIAL CONSISTENCY <level>` session commands.
func (c *cqlConn) setConsistency(query string) (handled bool, err error) {
	s := strings.ToLower(strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(query), ";")))
	if rest, found := strings.CutPrefix(s, "serial consistency"); found {
		level, ok := serialNames[strings.TrimSpace(rest)]
		if !ok {
			return true, fmt.Errorf("cql: unknown serial consistency %q", strings.TrimSpace(rest))
		}
		c.serial = &level
		return true, nil
	}
	rest, found := strings.CutPrefix(s, "consistency")
	if !found {
		return false, nil
	}
	name := strings.TrimSpace(rest)
	// SERIAL is also legal as a read consistency.
	if level, ok := serialNames[name]; ok {
		read := gocql.Consistency(level)
		c.consistency = &read
		return true, nil
	}
	level, ok := consistencyNames[name]
	if !ok {
		return true, fmt.Errorf("cql: unknown consistency %q", name)
	}
	c.consistency = &level
	return true, nil
}

func (c *cqlConn) Close() error {
	c.session.Close()
	return nil
}

func (c *cqlConn) Begin() (driver.Tx, error) {
	return nil, errors.New("cql: Cassandra has no session transactions; " +
		"use BEGIN BATCH or BEGIN TRANSACTION as a single statement")
}

func (c *cqlConn) Prepare(query string) (driver.Stmt, error) {
	return &cqlStmt{conn: c, query: query}, nil
}

func (c *cqlConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if handled, err := c.setConsistency(query); handled {
		return driver.RowsAffected(0), err
	}
	if err := c.query(ctx, query, args).Exec(); err != nil {
		return nil, err
	}
	return driver.RowsAffected(0), nil
}

// QueryContext materialises the whole result rather than holding an open
// iterator across step boundaries.
func (c *cqlConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if handled, err := c.setConsistency(query); handled {
		return &cqlRows{}, err
	}
	q := c.query(ctx, query, args)
	if isConditionalWrite(query) {
		return casRows(q)
	}
	iter := q.Iter()

	columns := iter.Columns()
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}

	var data [][]driver.Value
	for {
		cells := make([]any, len(columns))
		targets := make([]any, len(columns))
		for i := range cells {
			targets[i] = &cells[i]
		}
		if !iter.Scan(targets...) {
			break
		}
		row := make([]driver.Value, len(cells))
		for i, cell := range cells {
			row[i] = normalize(cell)
		}
		data = append(data, row)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return &cqlRows{names: names, data: data}, nil
}

// isConditionalWrite reports whether a statement is a lightweight
// transaction, whose result is an [applied] flag rather than rows. An Accord
// BEGIN TRANSACTION also carries an IF but returns the rows its own SELECT
// projected, so it is not one.
func isConditionalWrite(query string) bool {
	s := strings.ToLower(strings.TrimSpace(query))
	if strings.HasPrefix(s, "begin transaction") {
		return false
	}
	switch {
	case strings.HasPrefix(s, "update"),
		strings.HasPrefix(s, "insert"),
		strings.HasPrefix(s, "delete"),
		strings.HasPrefix(s, "begin batch"),
		strings.HasPrefix(s, "begin unlogged batch"),
		strings.HasPrefix(s, "begin counter batch"):
	default:
		return false
	}
	return strings.Contains(s, " if ")
}

// casRows runs a lightweight transaction and reports the outcome as a single
// row: [applied], followed by the columns Cassandra returns when it did not
// apply.
func casRows(q *gocql.Query) (driver.Rows, error) {
	existing := map[string]any{}
	applied, err := q.MapScanCAS(existing)
	if err != nil {
		return nil, err
	}
	names := []string{"[applied]"}
	row := []driver.Value{applied}
	for _, name := range sortedKeys(existing) {
		names = append(names, name)
		row = append(row, normalize(existing[name]))
	}
	return &cqlRows{names: names, data: [][]driver.Value{row}}, nil
}

func sortedKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// normalize maps gocql's Go types onto things database/sql will carry.
// Collections pass through as-is.
func normalize(v any) driver.Value {
	switch t := v.(type) {
	case int:
		return int64(t)
	case int32:
		return int64(t)
	case int16:
		return int64(t)
	case int8:
		return int64(t)
	case uint:
		return int64(t)
	default:
		return t
	}
}

func namedToValues(args []driver.NamedValue) []any {
	if len(args) == 0 {
		return nil
	}
	out := make([]any, len(args))
	for i, a := range args {
		out[i] = a.Value
	}
	return out
}

type cqlStmt struct {
	conn  *cqlConn
	query string
}

func (s *cqlStmt) Close() error  { return nil }
func (s *cqlStmt) NumInput() int { return -1 }

func (s *cqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.ExecContext(context.Background(), s.query, valuesToNamed(args))
}

func (s *cqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.QueryContext(context.Background(), s.query, valuesToNamed(args))
}

func (s *cqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *cqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

func valuesToNamed(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

type cqlRows struct {
	names []string
	data  [][]driver.Value
	pos   int
}

func (r *cqlRows) Columns() []string { return r.names }
func (r *cqlRows) Close() error      { return nil }

func (r *cqlRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}
