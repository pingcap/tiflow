package main

import (
	"database/sql"
	"strings"

	"go.uber.org/zap"
)

type DBHelper struct {
	db   *sql.DB
	kind Kind
}

func NewDBHelper(kind Kind) *DBHelper {
	return &DBHelper{
		db:   nil,
		kind: kind,
	}
}

func (h *DBHelper) MustOpen(connStringPattern string, dbName string) {
	connString := strings.Replace(connStringPattern, "{db}", dbName, -1)
	db, err := sql.Open("mysql", connString)
	if err != nil {
		logger.Panic(
			"Failed to open db",
			zap.String("kind", string(h.kind)),
			zap.String("conn", connString),
			zap.Error(err))
	}
	err = db.Ping()
	if err != nil {
		logger.Panic(
			"Failed to open db",
			zap.String("kind", string(h.kind)),
			zap.String("conn", connString),
			zap.Error(err))
	}
	h.db = db
}

func (h *DBHelper) MustExec(query string) {
	_, err := h.db.Exec(query)
	if err != nil {
		logger.Panic(
			"Failed to execute query",
			zap.String("kind", string(h.kind)),
			zap.String("query", query),
			zap.Error(err))
	}
}

func (h *DBHelper) Exec(query string) error {
	_, err := h.db.Exec(query)
	return err
}

func (h *DBHelper) MustClose() {
	err := h.db.Close()
	if err != nil {
		logger.Panic(
			"Failed to close connection",
			zap.String("kind", string(h.kind)),
			zap.Error(err))
	}
}
