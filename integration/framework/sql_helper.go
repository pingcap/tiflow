package framework

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"strings"
	"upper.io/db.v3/lib/sqlbuilder"
	_ "upper.io/db.v3/mysql"
)

type SqlHelper struct {
	upstream   *sql.DB
	downstream *sql.DB
	ctx context.Context
}

type Table struct {
	err error
	tableName string
	uniqueIndex string
	helper *SqlHelper
}

func (h *SqlHelper) GetTable(tableName string) *Table {
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

func (t *Table) Insert(rowData map[string]interface{}) Sender {
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

	return &SyncSqlRequest{basicReq}
}

type sqlRowContainer interface {
	getData() map[string]interface{}
	getTable() *Table
}

type awaitableSqlRowContainer struct {
	Awaitable
	sqlRowContainer
}

type sqlRequest struct {
	tableName   string
	// cols        []string
	data        map[string]interface{}
	result      map[string]interface{}
	uniqueIndex string
	helper      *SqlHelper
}

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

func (s *sqlRequest) getAwaitableSqlRowContainer() *awaitableSqlRowContainer {
	return &awaitableSqlRowContainer{
		Awaitable:       &basicAwaitable{
			pollableAndCheckable: s,
			timeout:              0,
		},
		sqlRowContainer: s,
	}
}

type Sender interface {
	Send() Awaitable
}

type errorSender struct {
	err error
}

func (s *errorSender) Send() Awaitable {
	return &errorCheckableAndAwaitable{s.err}
}

type SyncSqlRequest struct {
	sqlRequest
}

func (r *SyncSqlRequest) Send() Awaitable {
	err := r.insert(r.helper.ctx)
	if err != nil {
		return &errorCheckableAndAwaitable{errors.AddStack(err)}
	}
	return r.getAwaitableSqlRowContainer()
}

type AsyncSqlRequest struct {
	sqlRequest
}

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
	row, err := db.QueryRowContext(ctx, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = ? " +
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
