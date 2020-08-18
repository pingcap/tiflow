// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package framework

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"upper.io/db.v3/lib/sqlbuilder"
	_ "upper.io/db.v3/mysql" // imported for side effects
)

// SQLHelper provides basic utilities for manipulating data
type SQLHelper struct {
	upstream   *sql.DB
	downstream *sql.DB
	ctx        context.Context
}

// Table represents the handle of a table in the upstream
type Table struct {
	err         error
	tableName   string
	uniqueIndex string
	helper      *SQLHelper
}

// GetTable returns the handle of the given table
func (h *SQLHelper) GetTable(tableName string) *Table {
	db, err := sqlbuilder.New("mysql", h.upstream)
	if err != nil {
		return &Table{err: errors.AddStack(err)}
	}

	idxCol, err := getUniqueIndexColumn(h.ctx, db, "testdb", tableName)
	if err != nil {
		return &Table{err: errors.AddStack(err)}
	}

	return &Table{tableName: tableName, uniqueIndex: idxCol, helper: h}
}

// Insert returns a Sendable object that represents an Insert clause
func (t *Table) Insert(rowData map[string]interface{}) Sendable {
	if t.err != nil {
		return &errorSender{errors.AddStack(t.err)}
	}

	basicReq := sqlRequest{
		tableName:   t.tableName,
		data:        rowData,
		result:      nil,
		uniqueIndex: t.uniqueIndex,
		helper:      t.helper,
	}

	return &syncSQLRequest{basicReq}
}

type sqlRowContainer interface {
	getData() map[string]interface{}
	getTable() *Table
}

type awaitableSQLRowContainer struct {
	Awaitable
	sqlRowContainer
}

type sqlRequestType int32

const (
	sqlRequestTypeInsert sqlRequestType = iota
	sqlRequestTypeUpdate
	sqlRequestTypeDelete
)

type sqlRequest struct {
	tableName   string
	data        map[string]interface{}
	result      map[string]interface{}
	uniqueIndex string
	helper      *SQLHelper
	requestType sqlRequestType
}

// MarshalLogObjects helps printing the sqlRequest
func (s *sqlRequest) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("upstream", fmt.Sprintf("%#v", s.data))
	encoder.AddString("downstream", fmt.Sprintf("%#v", s.result))
	return nil
}

func (s *sqlRequest) getData() map[string]interface{} {
	return s.data
}

func (s *sqlRequest) getTable() *Table {
	return &Table{
		err:         nil,
		tableName:   s.tableName,
		uniqueIndex: s.uniqueIndex,
		helper:      s.helper,
	}
}

func (s *sqlRequest) getAwaitableSQLRowContainer() *awaitableSQLRowContainer {
	return &awaitableSQLRowContainer{
		Awaitable: &basicAwaitable{
			pollableAndCheckable: s,
			timeout:              0,
		},
		sqlRowContainer: s,
	}
}

// Sendable is a sendable request to the upstream
type Sendable interface {
	Send() Awaitable
}

type errorSender struct {
	err error
}

// Send implements sender
func (s *errorSender) Send() Awaitable {
	return &errorCheckableAndAwaitable{s.err}
}

type syncSQLRequest struct {
	sqlRequest
}

func (r *syncSQLRequest) Send() Awaitable {
	err := r.insert(r.helper.ctx)
	if err != nil {
		return &errorCheckableAndAwaitable{errors.AddStack(err)}
	}
	return r.getAwaitableSQLRowContainer()
}

/*
type asyncSQLRequest struct {
	sqlRequest
}
*/

func (s *sqlRequest) insert(ctx context.Context) error {
	db, err := sqlbuilder.New("mysql", s.helper.upstream)
	if err != nil {
		return errors.AddStack(err)
	}

	keys := make([]string, len(s.data))
	values := make([]interface{}, len(s.data))
	i := 0
	for k, v := range s.data {
		keys[i] = k
		values[i] = v
		i++
	}

	_, err = db.InsertInto(s.tableName).Columns(keys...).Values(values...).ExecContext(ctx)
	if err != nil {
		return errors.AddStack(err)
	}

	return nil
}

func (s *sqlRequest) read(ctx context.Context) (map[string]interface{}, error) {
	db, err := sqlbuilder.New("mysql", s.helper.downstream)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	rows, err := db.SelectFrom(s.tableName).Where(s.uniqueIndex+" = ?", s.data[s.uniqueIndex]).QueryContext(ctx)
	if err != nil {
		return nil, errors.AddStack(err)

	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}
	return rowsToMap(rows)
}

//nolint:unused
func (s *sqlRequest) getBasicAwaitable() basicAwaitable {
	return basicAwaitable{
		pollableAndCheckable: s,
		timeout:              0,
	}
}

func (s *sqlRequest) poll(ctx context.Context) (bool, error) {
	res, err := s.read(ctx)

	if err != nil {
		if strings.Contains(err.Error(), "Error 1146") {
			return false, nil
		}
		return false, errors.AddStack(err)
	}
	s.result = res
	return true, nil
}

func (s *sqlRequest) Check() error {
	// TODO better comparator
	if s.result == nil {
		return errors.New("Check: nil result")
	}
	if compareMaps(s.data, s.result) {
		return nil
	}
	log.Warn("Check failed", zap.Object("request", s))
	return errors.New("Check failed")
}

func rowsToMap(rows *sql.Rows) (map[string]interface{}, error) {
	colNames, err := rows.Columns()
	if err != nil {
		return nil, errors.AddStack(err)
	}

	colData := make([]interface{}, len(colNames))
	colDataPtrs := make([]interface{}, len(colNames))
	for i := range colData {
		colDataPtrs[i] = &colData[i]
	}

	err = rows.Scan(colDataPtrs...)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	ret := make(map[string]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		ret[colNames[i]] = colData[i]
	}
	return ret, nil
}

func getUniqueIndexColumn(ctx context.Context, db sqlbuilder.Database, dbName string, tableName string) (string, error) {
	row, err := db.QueryRowContext(ctx, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? "+
		"AND TABLE_NAME = ? AND NON_UNIQUE = 0",
		dbName, tableName)
	if err != nil {
		return "", errors.AddStack(err)
	}

	colName := ""
	err = row.Scan(&colName)
	if err != nil {
		return "", errors.AddStack(err)
	}

	return colName, nil
}

func compareMaps(m1 map[string]interface{}, m2 map[string]interface{}) bool {
	// TODO better comparator
	if m2 == nil {
		return false
	}
	str1 := fmt.Sprintf("%v", m1)
	str2 := fmt.Sprintf("%v", m2)
	return str1 == str2
}
