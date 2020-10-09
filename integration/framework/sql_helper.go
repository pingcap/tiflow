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
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/quotes"
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
	uniqueIndex []string
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

func (t *Table) makeSQLRequest(requestType sqlRequestType, rowData map[string]interface{}) (*sqlRequest, error) {
	if t.err != nil {
		return nil, t.err
	}

	return &sqlRequest{
		tableName:   t.tableName,
		data:        rowData,
		result:      nil,
		uniqueIndex: t.uniqueIndex,
		helper:      t.helper,
		requestType: requestType,
	}, nil
}

// Insert returns a Sendable object that represents an Insert clause
func (t *Table) Insert(rowData map[string]interface{}) Sendable {
	basicReq, err := t.makeSQLRequest(sqlRequestTypeInsert, rowData)
	if err != nil {
		return &errorSender{err: err}
	}

	return &syncSQLRequest{*basicReq}
}

// Upsert returns a Sendable object that represents a Replace Into clause
func (t *Table) Upsert(rowData map[string]interface{}) Sendable {
	basicReq, err := t.makeSQLRequest(sqlRequestTypeUpsert, rowData)
	if err != nil {
		return &errorSender{err: err}
	}

	return &syncSQLRequest{*basicReq}
}

// Delete returns a Sendable object that represents a Delete from clause
func (t *Table) Delete(rowData map[string]interface{}) Sendable {
	basicReq, err := t.makeSQLRequest(sqlRequestTypeDelete, rowData)
	if err != nil {
		return &errorSender{err: err}
	}

	return &syncSQLRequest{*basicReq}
}

type sqlRowContainer interface {
	getData() map[string]interface{}
	getComparableKey() string
	getTable() *Table
}

type awaitableSQLRowContainer struct {
	Awaitable
	sqlRowContainer
}

type sqlRequestType int32

const (
	sqlRequestTypeInsert sqlRequestType = iota
	sqlRequestTypeUpsert
	sqlRequestTypeDelete
)

type sqlRequest struct {
	tableName   string
	data        map[string]interface{}
	result      map[string]interface{}
	uniqueIndex []string
	helper      *SQLHelper
	requestType sqlRequestType
	hasReadBack uint32
}

// MarshalLogObjects helps printing the sqlRequest
func (s *sqlRequest) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("upstream", fmt.Sprintf("%#v", s.data))
	encoder.AddString("downstream", fmt.Sprintf("%#v", s.result))
	return nil
}

func (s *sqlRequest) getPrimaryKeyTuple() string {
	return makeColumnTuple(s.uniqueIndex)
}

func (s *sqlRequest) getWhereCondition() []interface{} {
	builder := strings.Builder{}
	args := make([]interface{}, 1, len(s.uniqueIndex)+1)
	builder.WriteString(s.getPrimaryKeyTuple() + " = (")
	for i, col := range s.uniqueIndex {
		builder.WriteString("?")
		if i != len(s.uniqueIndex)-1 {
			builder.WriteString(",")
		}

		args = append(args, s.data[col])
	}
	builder.WriteString(")")
	args[0] = builder.String()
	return args
}

func (s *sqlRequest) getComparableKey() string {
	if len(s.uniqueIndex) == 1 {
		return s.uniqueIndex[0]
	}

	ret := make(map[string]interface{})
	for k, v := range s.data {
		for _, col := range s.uniqueIndex {
			if k == col {
				ret[k] = v
			}
		}
	}
	return fmt.Sprintf("%v", ret)
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
			timeout:              30 * time.Second,
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
	atomic.StoreUint32(&r.hasReadBack, 0)
	var err error
	switch r.requestType {
	case sqlRequestTypeInsert:
		err = r.insert(r.helper.ctx)
	case sqlRequestTypeUpsert:
		err = r.upsert(r.helper.ctx)
	case sqlRequestTypeDelete:
		err = r.delete(r.helper.ctx)
	}

	go func() {
		db, err := sqlbuilder.New("mysql", r.helper.upstream)
		if err != nil {
			log.Warn("ReadBack:", zap.Error(err))
			return
		}

		cond := r.getWhereCondition()

		rows, err := db.SelectFrom(r.tableName).Where(cond).QueryContext(r.helper.ctx)
		if err != nil {
			log.Warn("ReadBack:", zap.Error(err))
			return
		}
		defer rows.Close()

		if !rows.Next() {
			// Upstream does not have the row
			if r.requestType != sqlRequestTypeDelete {
				log.Warn("ReadBack: no row, likely to be bug")
			}
		} else {
			r.data, err = rowsToMap(rows)
			if err != nil {
				log.Warn("ReadBack", zap.Error(err))
				return
			}
		}

		atomic.StoreUint32(&r.hasReadBack, 1)
	}()

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

	s.requestType = sqlRequestTypeInsert
	return nil
}

func (s *sqlRequest) upsert(ctx context.Context) error {
	db := sqlx.NewDb(s.helper.upstream, "mysql")

	keys := make([]string, len(s.data))
	values := make([]interface{}, len(s.data))
	i := 0
	for k, v := range s.data {
		keys[i] = k
		values[i] = v
		i++
	}

	query, args, err := sqlx.In("replace into `"+s.tableName+"` "+makeColumnTuple(keys)+" values (?)", values)
	if err != nil {
		return errors.AddStack(err)
	}

	query = db.Rebind(query)
	_, err = s.helper.upstream.ExecContext(ctx, query, args...)
	if err != nil {
		return errors.AddStack(err)
	}

	s.requestType = sqlRequestTypeUpsert
	return nil
}

func (s *sqlRequest) delete(ctx context.Context) error {
	db, err := sqlbuilder.New("mysql", s.helper.upstream)
	if err != nil {
		return errors.AddStack(err)
	}

	_, err = db.DeleteFrom(s.tableName).Where(s.getWhereCondition()).ExecContext(ctx)
	if err != nil {
		return errors.AddStack(err)
	}

	s.requestType = sqlRequestTypeDelete
	return nil
}

func (s *sqlRequest) read(ctx context.Context) (map[string]interface{}, error) {
	db, err := sqlbuilder.New("mysql", s.helper.downstream)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	rows, err := db.SelectFrom(s.tableName).Where(s.getWhereCondition()).QueryContext(ctx)
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
	if atomic.LoadUint32(&s.hasReadBack) == 0 {
		return false, nil
	}
	res, err := s.read(ctx)
	if err != nil {
		if strings.Contains(err.Error(), "Error 1146") {
			return false, nil
		}
		return false, errors.AddStack(err)
	}
	s.result = res

	switch s.requestType {
	case sqlRequestTypeInsert:
		if res == nil {
			return false, nil
		}
		return true, nil
	case sqlRequestTypeUpsert:
		if res == nil {
			return false, nil
		}
		if compareMaps(s.data, res) {
			return true, nil
		}
		log.Debug("Upserted row does not match the expected")
		return false, nil
	case sqlRequestTypeDelete:
		if res == nil {
			return true, nil
		}
		log.Debug("Delete not successful yet", zap.Reflect("where condition", s.getWhereCondition()))
		return false, nil
	}
	return true, nil
}

func (s *sqlRequest) Check() error {
	if s.requestType == sqlRequestTypeUpsert || s.requestType == sqlRequestTypeDelete {
		return nil
	}
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

func getUniqueIndexColumn(ctx context.Context, db sqlbuilder.Database, dbName string, tableName string) ([]string, error) {
	row, err := db.QueryRowContext(ctx, `
		SELECT GROUP_CONCAT(COLUMN_NAME SEPARATOR ' ') FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		GROUP BY INDEX_NAME
		ORDER BY FIELD(INDEX_NAME,'PRIMARY') DESC
	`, dbName, tableName)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	colName := ""
	err = row.Scan(&colName)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	return strings.Split(colName, " "), nil
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

func makeColumnTuple(colNames []string) string {
	colNamesQuoted := make([]string, len(colNames))
	for i := range colNames {
		colNamesQuoted[i] = quotes.QuoteName(colNames[i])
	}
	return "(" + strings.Join(colNamesQuoted, ",") + ")"
}
