package main

import _ "github.com/go-sql-driver/mysql"

func main() {}

var mysqlLevels = map[string]string{
	"read-uncommitted": "READ UNCOMMITTED",
	"read-committed":   "READ COMMITTED",
	"repeatable-read":  "REPEATABLE READ",
	"serializable":     "SERIALIZABLE",
}

func IsolationSQL(level string) string {
	l, ok := mysqlLevels[level]
	if !ok {
		return ""
	}
	return "SET TRANSACTION ISOLATION LEVEL " + l
}

func ShowIsolationSQL() string {
	return "SELECT @@transaction_isolation"
}
