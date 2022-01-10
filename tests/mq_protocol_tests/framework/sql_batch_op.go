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
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	selectQueryMaxBatchSize = 256
)

type sqlAllAwaiter struct {
	helper          *SQLHelper
	data            map[interface{}]sqlRowContainer
	retrievedValues []map[string]interface{}
	table           *Table
}

// All joins a slice of Awaitable sql requests. The request must be to the same table.
// TODO does not support composite primary key for now!
func All(helper *SQLHelper, awaitables []Awaitable) Awaitable {
	if _, ok := awaitables[0].(sqlRowContainer); !ok {
		return awaitables[0]
	}

	ret := &sqlAllAwaiter{
		helper:          helper,
		data:            make(map[interface{}]sqlRowContainer, len(awaitables)),
		retrievedValues: make([]map[string]interface{}, 0),
		table:           awaitables[0].(sqlRowContainer).getTable(),
	}

	for _, row := range awaitables {
		rowContainer, ok := row.(sqlRowContainer)
		if !ok {
			return row
		}
		key := rowContainer.getData()[rowContainer.getTable().uniqueIndex[0]]
		ret.data[normalizeKeys(key)] = rowContainer
	}

	return &basicAwaitable{
		pollableAndCheckable: ret,
		timeout:              120 * time.Second,
	}
}

func (s *sqlAllAwaiter) poll(ctx context.Context) (bool, error) {
	db := sqlx.NewDb(s.helper.downstream, "mysql")

	batchSize := 0
	counter := 0
	indexValues := make([]interface{}, 0)
	s.retrievedValues = make([]map[string]interface{}, 0)
	for k, v := range s.data {
		indexValues = append(indexValues, k)
		batchSize++
		counter++
		if batchSize >= selectQueryMaxBatchSize || counter == len(s.data) {
			log.Debug("Selecting", zap.String("table", s.table.tableName), zap.Any("keys", indexValues))
			query, args, err := sqlx.In("select distinct * from "+s.table.tableName+" where "+v.getTable().uniqueIndex[0]+" in (?)", indexValues)
			if err != nil {
				return false, errors.AddStack(err)
			}
			query = db.Rebind(query)
			rows, err := db.QueryContext(ctx, query, args...)
			if err != nil {
				if strings.Contains(err.Error(), "Error 1146") {
					log.Info("table does not exist, will try again", zap.Error(err), zap.String("query", query))
					return false, nil
				}
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
			indexValues = make([]interface{}, 0)
		}
	}

	log.Debug("poll finished", zap.Int("totalRetrieved", len(s.retrievedValues)))

	if len(s.data) == len(s.retrievedValues) {
		return true, nil
	}

	return false, nil
}

func (s *sqlAllAwaiter) Check() error {
	for _, row := range s.retrievedValues {
		key := row[s.table.uniqueIndex[0]]
		expected := s.data[normalizeKeys(key)]
		if !compareMaps(row, expected.getData()) {
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

func normalizeKeys(key interface{}) interface{} {
	switch key.(type) {
	case int, int8, int16, int32, int64:
		return reflect.ValueOf(key).Int()
	case uint, uint8, uint16, uint32, uint64:
		return reflect.ValueOf(key).Uint()
	default:
		return key
	}
}
