// The cql plugin drives Cassandra through the database/sql adapter in
// driver.go.
package main

func main() {}

// Cassandra has no transaction isolation levels, so there is nothing for a
// level to select. What differs between the Cassandra workloads is the
// statement form (plain, batch, LWT, transaction), and that lives in the
// script, not in a session setting.
//
// The runner treats an empty return as "unknown isolation level" and aborts,
// and executes whatever is returned on every client connection. So the one
// accepted level maps to a harmless read, and anything else is rejected,
// which is what catches `serializable` being passed here by habit.
func IsolationSQL(level string) string {
	switch level {
	case "none", "":
		return "SELECT release_version FROM system.local"
	}
	return ""
}

func ShowIsolationSQL() string {
	return "SELECT release_version FROM system.local"
}
