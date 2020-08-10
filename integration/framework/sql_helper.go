package framework

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"reflect"

	"upper.io/db.v3/lib/sqlbuilder"
)

type SqlHelper struct {
	upstream   *sql.DB
	downstream *sql.DB
}

type Table struct {
	err error
}

func (h *SqlHelper) GetTable(tableName string) *Table {
	db, err := sqlbuilder.New("mysql", h.upstream)
	if err != nil {
		return &Table{err: err}
	}

	
}

type sqlRequest struct {
	tableName   string
	cols        []string
	data        map[string]interface{}
	result      map[string]interface{}
	uniqueIndex string
	helper      *SqlHelper
}

func (s *sqlRequest) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	encoder.AddString("upstream", fmt.Sprintf("%v", s.data))
	encoder.AddString("downstream", fmt.Sprintf("%v", s.result))
	return nil
}

type SyncSqlRequest struct {
	sqlRequest
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
	db, err := sqlbuilder.New("mysql", s.helper.upstream)
	if err != nil {
		return nil, errors.AddStack(err)
	}


	rows, err := db.SelectFrom(s.tableName).Where(s.uniqueIndex + " = ?", s.data[s.uniqueIndex]).QueryContext(ctx)
	if err != nil {
		return nil, errors.AddStack(err)

	}
	defer rows.Close()

	return rowsToMap(rows)
}

func (s *sqlRequest) getBasicAwaitable() basicAwaitable {
	return basicAwaitable{
		pollableAndCheckable: s,
		timeout:  0,
	}
}

func (s *sqlRequest) poll(ctx context.Context) (bool, error) {
	res, err := s.read(ctx)
	if err != nil {
		return false, errors.AddStack(err)
	}
	s.result = res
	return true, nil
}

func (s *sqlRequest) Check() error {
	if reflect.DeepEqual(s.data, s.result) {
		return nil
	}
	log.Warn("Check failed", zap.Object("request", s))
	return errors.New("Check failed")
}

func (r *SyncSqlRequest) Send() Awaitable {
	err := r.insert(context.Background())
	if err != nil {

	}
	return &basicAwaitable{
		pollableAndCheckable: r,
		timeout:   0,
	}
}

func rowsToMap(rows *sql.Rows) (map[string]interface{}, error) {
	colNames, err := rows.Columns()
	if err != nil {
		return nil, errors.AddStack(err)
	}

	colData := make([]interface{}, len(colNames))
	if !rows.Next() {
		return nil, nil
	}

	err = rows.Scan(colData...)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	ret := make(map[string]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		ret[colNames[i]] = colData[i]
	}
	return ret, nil
}
