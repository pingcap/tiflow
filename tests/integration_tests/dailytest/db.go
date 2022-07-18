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

package dailytest

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/tests/integration_tests/util"
	"go.uber.org/zap/zapcore"
)

func intRangeValue(column *column, min int64, max int64) (int64, int64) {
	var err error
	if len(column.min) > 0 {
		min, err = strconv.ParseInt(column.min, 10, 64)
		if err != nil {
			log.S().Fatal(err)
		}

		if len(column.max) > 0 {
			max, err = strconv.ParseInt(column.max, 10, 64)
			if err != nil {
				log.S().Fatal(err)
			}
		}
	}

	return min, max
}

func randInt64Value(column *column, min int64, max int64) int64 {
	if len(column.set) > 0 {
		idx := randInt(0, len(column.set)-1)
		data, _ := strconv.ParseInt(column.set[idx], 10, 64)
		return data
	}

	min, max = intRangeValue(column, min, max)
	return randInt64(min, max)
}

func uniqInt64Value(column *column, max int64) int64 {
	min, max := intRangeValue(column, 0, max)
	column.data.setInitInt64Value(column.step, min, max)
	return column.data.uniqInt64()
}

func queryCount(table *table, db *sql.DB) (int, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT COUNT(*) as count FROM %s", table.name))
	if err != nil {
		return 0, errors.Trace(err)
	}

	var nums int
	for rows.Next() {
		err = rows.Scan(&nums)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	return nums, nil
}

func genDeleteSqls(table *table, db *sql.DB, count int) ([]string, [][]interface{}, error) {
	nums, err := queryCount(table, db)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var sqls []string
	var args [][]interface{}

	if nums == 0 || nums-count < 1 {
		return sqls, args, nil
	}

	start := randInt(1, nums-count)
	length := len(table.columns)
	where := genWhere(table.columns)

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s limit %d, %d", table.name, start, count))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for rows.Next() {
		data := make([]interface{}, length)
		dbArgs := make([]interface{}, length)

		for i := 0; i < length; i++ {
			dbArgs[i] = &data[i]
		}

		err = rows.Scan(dbArgs...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		sqls = append(sqls, fmt.Sprintf("delete from %s where %s", table.name, where))
		args = append(args, data)
	}

	return sqls, args, nil
}

func genUpdateSqls(table *table, db *sql.DB, count int) ([]string, [][]interface{}, error) {
	nums, err := queryCount(table, db)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	var sqls []string
	var args [][]interface{}

	if nums == 0 || nums-count < 1 {
		return sqls, args, nil
	}

	start := randInt(1, nums-count)
	length := len(table.columns)
	where := genWhere(table.columns)

	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s limit %d, %d", table.name, start, count))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for rows.Next() {
		data := make([]interface{}, length)
		dbArgs := make([]interface{}, length)

		for i := 0; i < length; i++ {
			dbArgs[i] = &data[i]
		}

		err = rows.Scan(dbArgs...)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		index := randInt(2, length-1)
		column := table.columns[index]
		updateData, err := genColumnData(table, column)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		sqls = append(sqls, fmt.Sprintf("update %s set `%s` = %s where %s", table.name, column.name, updateData, where))
		args = append(args, data)
	}

	return sqls, args, nil
}

func genInsertSqls(table *table, count int) ([]string, [][]interface{}, error) {
	datas := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	for i := 0; i < count; i++ {
		data, err := genRowData(table)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		datas = append(datas, data)
		args = append(args, nil)
	}

	return datas, args, nil
}

func genWhere(columns []*column) string {
	var kvs bytes.Buffer
	for i := range columns {
		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` = ?", columns[i].name)
		} else {
			fmt.Fprintf(&kvs, "`%s` = ? and ", columns[i].name)
		}
	}

	return kvs.String()
}

func genRowData(table *table) (string, error) {
	var values []byte
	for _, column := range table.columns {
		data, err := genColumnData(table, column)
		if err != nil {
			return "", errors.Trace(err)
		}
		values = append(values, []byte(data)...)
		values = append(values, ',')
	}

	values = values[:len(values)-1]
	sql := fmt.Sprintf("insert into %s  values (%s);", table.name, string(values))
	return sql, nil
}

func genColumnData(table *table, column *column) (string, error) {
	tp := column.tp
	_, isUnique := table.uniqIndices[column.name]
	isUnsigned := mysql.HasUnsignedFlag(tp.GetFlag())

	switch tp.GetType() {
	case mysql.TypeTiny:
		var data int64
		if isUnique {
			data = uniqInt64Value(column, math.MaxUint8)
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint8)
			} else {
				data = randInt64Value(column, math.MinInt8, math.MaxInt8)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeShort:
		var data int64
		if isUnique {
			data = uniqInt64Value(column, math.MaxUint16)
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint16)
			} else {
				data = randInt64Value(column, math.MinInt16, math.MaxInt16)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeLong:
		var data int64
		if isUnique {
			data = uniqInt64Value(column, math.MaxUint32)
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxUint32)
			} else {
				data = randInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeLonglong:
		var data int64
		if isUnique {
			data = uniqInt64Value(column, math.MaxInt64)
		} else {
			if isUnsigned {
				data = randInt64Value(column, 0, math.MaxInt64)
			} else {
				data = randInt64Value(column, math.MinInt32, math.MaxInt32)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqString(tp.GetFlen()))...)
		} else {
			data = append(data, []byte(randString(randInt(1, tp.GetFlen())))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeFloat, mysql.TypeDouble:
		var data float64
		if isUnique {
			data = float64(uniqInt64Value(column, math.MaxInt64))
		} else {
			if isUnsigned {
				data = float64(randInt64Value(column, 0, math.MaxInt64))
			} else {
				data = float64(randInt64Value(column, math.MinInt32, math.MaxInt32))
			}
		}
		return strconv.FormatFloat(data, 'f', -1, 64), nil
	case mysql.TypeDate:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqDate())...)
		} else {
			data = append(data, []byte(randDate(column.min, column.max))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqTimestamp())...)
		} else {
			data = append(data, []byte(randTimestamp(column.min, column.max))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeDuration:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqTime())...)
		} else {
			data = append(data, []byte(randTime(column.min, column.max))...)
		}

		data = append(data, '\'')
		return string(data), nil
	case mysql.TypeYear:
		data := []byte{'\''}
		if isUnique {
			data = append(data, []byte(column.data.uniqYear())...)
		} else {
			data = append(data, []byte(randYear(column.min, column.max))...)
		}

		data = append(data, '\'')
		return string(data), nil
	default:
		return "", errors.Errorf("unsupported column type - %v", column)
	}
}

func execSQLs(db *sql.DB, sqls []string) error {
	for _, sql := range sqls {
		err := execSQL(db, sql)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func execSQL(db *sql.DB, sql string) error {
	if len(sql) == 0 {
		return nil
	}

	_, err := db.Exec(sql)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// RunTest will call writeSrc and check if src is contisitent with dst
func RunTest(src *sql.DB, dst *sql.DB, schema string, writeSrc func(src *sql.DB)) {
	writeSrc(src)

	tick := time.NewTicker(time.Second * 5)
	defer tick.Stop()
	timeout := time.After(time.Second * 240)

	oldLevel := log.GetLevel()
	defer log.SetLevel(oldLevel)

	for {
		select {
		case <-tick.C:
			log.SetLevel(zapcore.WarnLevel)
			if util.CheckSyncState(src, dst, schema) {
				return
			}
		case <-timeout:
			// check last time
			log.SetLevel(zapcore.InfoLevel)
			if !util.CheckSyncState(src, dst, schema) {
				log.S().Fatal("sourceDB don't equal targetDB")
			}

			return
		}
	}
}
