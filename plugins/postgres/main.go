package main

import _ "github.com/lib/pq"

func main() {}

var pgLevels = map[string]string{
	"read-committed":  "READ COMMITTED",
	"repeatable-read": "REPEATABLE READ",
	"serializable":    "SERIALIZABLE",
}

func IsolationSQL(level string) string {
	l, ok := pgLevels[level]
	if !ok {
		return ""
	}
	return "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " + l
}
