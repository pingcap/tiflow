package framework

import (
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	//"upper.io/db.v3/lib/sqlbuilder"
)

const (
	selectQueryMaxBatchSize = 1024
)

type sqlAllAwaiter struct {
	helper          *SqlHelper
	data            map[interface{}]map[string]interface{}
	retrievedValues []map[string]interface{}
	table           *Table
}

func All(helper *SqlHelper, awaitables []Awaitable) Awaitable {
	ret := &sqlAllAwaiter{
		helper:          helper,
		data:            make(map[interface{}]map[string]interface{}, len(awaitables)),
		retrievedValues: make([]map[string]interface{}, 0),
		table:           awaitables[0].(sqlRowContainer).getTable(),
	}

	for _, row := range awaitables {
		row := row.(sqlRowContainer)
		key := row.getData()[row.getTable().uniqueIndex]
		value := row.getData()
		ret.data[key] = value
	}

	return &basicAwaitable{
		pollableAndCheckable: ret,
		timeout:              0,
	}
}

func (s *sqlAllAwaiter) poll(ctx context.Context) (bool, error) {
	//db, err := sqlbuilder.New("mysql", s.helper.downstream)
	//if err != nil {
	//	return false, errors.AddStack(err)
	//}

	batchSize := 0
	counter := 0
	indexValues := make([]interface{}, 0)
	for k, _ := range s.data {
		indexValues = append(indexValues, k)
		batchSize++
		counter++
		if batchSize >= selectQueryMaxBatchSize || counter == len(s.data) {
			log.Debug("Selecting", zap.String("table", s.table.tableName), zap.Any("keys", indexValues))
			//query :=  db.SelectFrom(s.table.tableName).
			//	Where("? IN ?", s.table.uniqueIndex, indexValues)
			rows, err := s.helper.downstream.Query("select from test where id = ?", indexValues)
			/*
			log.Debug("Query", zap.String("query", query.String()))
			rows, err := query.QueryContext(s.helper.ctx)
			*/

			if err != nil {
				return false, errors.AddStack(err)
			}

			for rows.Next() {
				m, err := rowsToMap(rows)
				if err != nil {
					return false, errors.AddStack(err)
				}
				s.retrievedValues = append(s.retrievedValues, m)
			}
			batchSize = 0
		}
	}

	log.Debug("poll finished", zap.Int("total-retrieved", len(s.retrievedValues)))

	if len(s.data) == len(s.retrievedValues) {
		return true, nil
	}

	return false, nil
}

func (s *sqlAllAwaiter) Check() error {
	for _, row := range s.retrievedValues {
		expected := s.data[row[s.table.uniqueIndex]]
		if !compareMaps(row, s.data[row[s.table.uniqueIndex]]) {
			log.Warn(
				"Check failed",
				zap.String("expected", fmt.Sprintf("%v", expected)),
				zap.String("actual", fmt.Sprintf("%v", row)),
			)
			return errors.New("Check failed")
		}
	}
	return nil
}
