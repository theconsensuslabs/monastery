# Monastery

A tool for testing and observing transaction behavior.

There are builtin scripts that use
[Hermitage](https://github.com/ept/hermitage) test cases in
[./hermitage](./hermitage)`.

For example to test the behavior of Predicate-Many-Preceders (PMP) for
write predicates in PostgreSQL at the repeatable-read isolation level.

```shell
$ initdb testdb
$ postgres -D testdb -p 4000
```

In another terminal.

```shell
$ git clone https://github.com/theconsensuslabs/monastery
$ cd monastery
$ go build -buildmode=plugin -o postgres.so ./plugins/postgres/
$ go build
$ ./monastery postgres 'host=localhost port=4000 sslmode=disable dbname=postgres' repeatable-read hermitage/06-pmp.sql
┌──────────┬────────────────────────────────────────────────────────────┬──────────────┬──────────────┬────────────────────┬────────────────────────────────────────┬──────────────────────────────────────────────────┐
│CLIENT    │COMMAND                                                     │STARTED       │ENDED         │RESULTS             │ERROR                                   │ASSERT                                            │
╞──────────╪────────────────────────────────────────────────────────────╪──────────────╪──────────────╪────────────────────╪────────────────────────────────────────╪──────────────────────────────────────────────────╡
│setup     │drop table if exists test;                                  │10:05:37.339  │10:05:37.340  │                    │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│setup     │create table test (id int primary key, value int);          │10:05:37.340  │10:05:37.342  │                    │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│setup     │insert into test (id, value) values (1, 10), (2, 20);       │10:05:37.343  │10:05:37.344  │                    │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t1        │begin;                                                      │10:05:37.346  │10:05:37.346  │                    │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t1        │SHOW transaction_isolation;                                 │10:05:37.647  │10:05:37.650  │{repeatable read}   │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t2        │begin;                                                      │10:05:37.947  │10:05:37.952  │                    │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t2        │SHOW transaction_isolation;                                 │10:05:38.247  │10:05:38.253  │{repeatable read}   │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t1        │select * from test where value = 30;                        │10:05:38.546  │10:05:38.554  │                    │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t2        │insert into test (id, value) values(3, 30);                 │10:05:38.847  │10:05:38.853  │                    │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t2        │commit;                                                     │10:05:39.147  │10:05:39.152  │                    │                                        │                                                  │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t1        │select * from test where value % 3 = 0;                     │10:05:39.447  │10:05:39.454  │                    │                                        │OK ()                                             │
├──────────┼────────────────────────────────────────────────────────────┼──────────────┼──────────────┼────────────────────┼────────────────────────────────────────┼──────────────────────────────────────────────────┤
│t1        │commit;                                                     │10:05:39.747  │10:05:39.754  │                    │                                        │                                                  │
└──────────┴────────────────────────────────────────────────────────────┴──────────────┴──────────────┴────────────────────┴────────────────────────────────────────┴──────────────────────────────────────────────────┘
5dd71860-a896-485f-be0f-c599885dfda9
```

These events also get emitted to `monastery.jsonl`. You can filter them for a run by the uuid above.

```terminal
jq  -c 'select(.run_id=="fe1de39c-a00b-476a-9a40-deb975e70a04")' monastery.jsonl
```

Or run with `-events-only` flag.

```terminal
$ ./monastery -events-only postgres 'host=localhost port=4000 sslmode=disable dbname=postgres' repeatable-read hermitage/06-pmp.sql
```

You can slow down the run using `-interactive` to trigger each event one-by-one or run with `-interval .05s` to go slowly.

## Running the full hermitage suite

`run-hermitage.sh` runs every script in `hermitage/` against every isolation
level in lexicographical order and prints a summary table:

```shell
$ ./run-hermitage.sh postgres 'host=localhost port=4000 sslmode=disable dbname=postgres'
./run-hermitage.sh postgres 'host=localhost port=4000 sslmode=disable dbname=postgres'
| Test                                         | Read Uncommitted   | Read Committed     | Repeatable Read    | Serializable       |
|----------------------------------------------|--------------------|--------------------|--------------------|--------------------|
| 01-g0-write-cycles-dirty-writes              | FAIL               | OK                 | OK                 | OK                 |
| 02-g1a-aborted-reads-dirty-reads             | FAIL               | OK                 | OK                 | OK                 |
| 03-g1b-intermediate-reads-dirty-reads        | FAIL               | OK                 | OK                 | OK                 |
| 04-g1c-circular-information-flow-dirty-reads | FAIL               | OK                 | OK                 | OK                 |
| 05-otv                                       | FAIL               | OK                 | OK                 | OK                 |
| 06-pmp                                       | FAIL               | FAIL               | OK                 | OK                 |
| 07-pmp-write-predicates                      | FAIL               | FAIL               | OK                 | OK                 |
| 08-p4-lost-update                            | FAIL               | FAIL               | OK                 | OK                 |
| 09-g-single-read-skew                        | FAIL               | FAIL               | OK                 | OK                 |
| 10-g-single-write-predicate                  | FAIL               | FAIL               | OK                 | OK                 |
| 11-g-single-predicate-read-skew              | FAIL               | FAIL               | OK                 | OK                 |
| 12-g2-item-write-skew                        | FAIL               | FAIL               | FAIL               | OK                 |
| 13-g2-predicate-read-write-skew              | FAIL               | FAIL               | FAIL               | OK                 |
| 14-g2-predicate-read-fekete-write-skew       | FAIL               | FAIL               | FAIL               | OK                 |
```

Per-run JSONL logs and stdout/stderr land in `.hermitage-logs/` for inspection.

## Assertions

Each step can carry an `assert` after a `--`. The result is shown in the
`ASSERT` column (green when it holds, red when it doesn't), and `monastery`
exits non-zero if any assertion fails.

| Form                            | Passes when                                |
|---------------------------------|--------------------------------------------|
| `-- assert error`               | the statement returned an error            |
| `-- assert ok`                  | the statement returned no error            |
| `-- assert ()`                  | the statement returned no rows             |
| `-- assert ({1, 10}, {2, 20})`  | the rows match exactly, no order           |

Alternatives can be chained with ` or ` and the assertion holds if any of
them matches — useful when a step is allowed to either succeed with a
specific result or fail with an error depending on the isolation level:

```sql
t1: select * from test;  -- assert ({1, 12}, {2, 22}) or ({1, 11}, {2, 21})
t2: commit;              -- assert ok or error
```

Anything after a `#` in the assert expression is treated as a free-text
note and discarded:

```sql
t2: select * from test;  -- assert ({1, 11}, {2, 20}) or ({1, 10}, {2, 20})  # latter under SI
```

## Group invariants

Per-step assertions hard-code which transaction the engine must abort.
That works for SSI (deterministic victim by commit order) but breaks on
S2PL implementations that resolve cycles via deadlock detection and may
pick a different victim. To assert "*some* transaction in this cycle
must abort, but I don't care which", tag each candidate step with
`-- group <name>`:

```sql
t1: update test set value = 0 where id = 1;          -- group cycle1
t2: update test set value = value + 5 where id = 2;  -- group cycle1
```

After the run, each named group is checked: at least one tagged step
must have errored. The result appears as a synthetic row in the table
(`group: at least one error`) and is logged as a `group_eval` event.

`group` and `assert` are composable, separated by `;`:

```sql
t1: commit; -- assert ok; group cycle1
```

Group checks are skipped when the run is interrupted (Ctrl-C), since a
missing member would spuriously fail the invariant.

## Per-driver script overrides

Some anomalies surface differently under SI/SSI vs. lock-based
serializable, and the right assertion depends on the engine. When
loading `foo.sql`, monastery first looks for `foo.sql.<driver>` (e.g.
`foo.sql.mysql`) and uses that file if it exists. The actual path
loaded is recorded in the `session_start` event's `script` field.

`run-hermitage.sh` doesn't need any changes — its `*.sql` glob won't
match `*.sql.<driver>`, but the Go runner does the resolution.
