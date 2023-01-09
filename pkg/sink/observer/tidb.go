// Copyright 2023 PingCAP, Inc.
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

package observer

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

var (
	// Query latest tidb connection idle duration, sample output:
	// +----------------------------+------------------+--------+---------------------+
	// | time                       | instance         | in_txn | value               |
	// +----------------------------+------------------+--------+---------------------+
	// | 2023-01-10 16:40:16.372000 | 10.2.6.127:11080 | 0      |  0.3125843212237097 |
	// | 2023-01-10 16:40:16.372000 | 10.2.6.127:11080 | 1      | 0.00049736527952178 |
	// +----------------------------+------------------+--------+---------------------+
	queryConnIdleDurationStmt = `SELECT a.time, a.instance, a.in_txn, a.value
	FROM METRICS_SCHEMA.tidb_connection_idle_duration a
	INNER JOIN (
		SELECT instance, in_txn, MAX(time) time FROM METRICS_SCHEMA.tidb_connection_idle_duration
		GROUP BY instance, in_txn
	) b ON a.instance = b.instance AND a.in_txn = b.in_txn AND a.time = b.time;`

	// Query latest tidb connection count, sample output:
	// +----------------------------+------------------+-------+
	// | time                       | instance         | value |
	// +----------------------------+------------------+-------+
	// | 2023-01-10 16:44:39.123000 | 10.2.6.127:11080 |    24 |
	// +----------------------------+------------------+-------+
	queryConnCountStmt = `SELECT a.time, a.instance, a.value
	FROM METRICS_SCHEMA.tidb_connection_count a
	INNER JOIN (
		SELECT instance, MAX(time) time FROM METRICS_SCHEMA.tidb_connection_count
		GROUP BY instance
	) b ON a.instance = b.instance AND a.time = b.time;`

	// Query latest tidb query duration, sample output:
	// +----------------------------+------------------+----------------+-----------------------+
	// | time                       | instance         | sql_type       | value                 |
	// +----------------------------+------------------+----------------+-----------------------+
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Begin          | 0.0018886375591793793 |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Commit         |  0.014228768066070199 |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | CreateDatabase |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | CreateTable    |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Delete         |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Execute        |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Insert         | 0.0004933262664880737 |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Replace        |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Rollback       |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Select         |   0.06080000000000001 |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Set            | 0.0017023494860499266 |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Show           |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Update         |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | Use            |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | general        |                  NULL |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | internal       |  0.007085714285714287 |
	// | 2023-01-10 16:47:08.283000 | 10.2.6.127:11080 | other          |                  NULL |
	// +----------------------------+------------------+----------------+-----------------------+
	queryQueryDurationStmt = `SELECT a.time, a.instance, a.sql_type, a.value
	FROM METRICS_SCHEMA.tidb_query_duration a
	INNER JOIN (
		SELECT instance, sql_type, MAX(time) time FROM METRICS_SCHEMA.tidb_query_duration
		GROUP BY instance, sql_type
	) b ON a.instance = b.instance AND a.sql_type = b.sql_type AND a.time = b.time;`

	// Query latest tidb transaction duration, sample output:
	// +----------------------------+------------------+----------+---------------------+
	// | time                       | instance         | type     | value               |
	// +----------------------------+------------------+----------+---------------------+
	// | 2023-01-10 16:50:38.153000 | 10.2.6.127:11080 | abort    |                NULL |
	// | 2023-01-10 16:50:38.153000 | 10.2.6.127:11080 | commit   | 0.06155323076923076 |
	// | 2023-01-10 16:50:38.153000 | 10.2.6.127:11080 | rollback |                NULL |
	// +----------------------------+------------------+----------+---------------------+
	queryTxnDurationStmt = `SELECT a.time, a.instance, a.type, a.value
	FROM METRICS_SCHEMA.tidb_transaction_duration a
	INNER JOIN (
		SELECT instance, type, MAX(time) time FROM METRICS_SCHEMA.tidb_transaction_duration
		GROUP BY instance, type
	) b ON a.instance = b.instance AND a.type = b.type AND a.time = b.time;`
)

// TiDBDiagnoser is a tidb performance observer
type TiDBObserver struct {
	db *sql.DB
}

// Tick implements Observer
func (o *TiDBObserver) Tick(ctx context.Context) error {
	m1 := make([]*tidbConnIdleDuration, 0)
	if err := queryMetrics[tidbConnIdleDuration](
		ctx, o.db, queryConnIdleDurationStmt, &m1); err != nil {
		return err
	}
	for _, m := range m1 {
		if !m.duration.Valid {
			m.duration.Float64 = 0
		}
		inTxnLabel := strconv.Itoa(m.inTxn)
		tidbConnIdleDurationGauge.WithLabelValues(m.instance, inTxnLabel).Set(m.duration.Float64)
	}

	m2 := make([]*tidbConnCount, 0)
	if err := queryMetrics[tidbConnCount](
		ctx, o.db, queryConnCountStmt, &m2); err != nil {
		return err
	}
	for _, m := range m2 {
		if !m.count.Valid {
			m.count.Int32 = 0
		}
		tidbConnCountGauge.WithLabelValues(m.instance).Set(float64(m.count.Int32))
	}

	m3 := make([]*tidbQueryDuration, 0)
	if err := queryMetrics[tidbQueryDuration](
		ctx, o.db, queryQueryDurationStmt, &m3); err != nil {
		return err
	}
	for _, m := range m3 {
		if !m.duration.Valid {
			m.duration.Float64 = 0
		}
		tidbQueryDurationGauge.WithLabelValues(m.instance, m.queryType).Set(m.duration.Float64)
	}

	m4 := make([]*tidbTxnDuration, 0)
	if err := queryMetrics[tidbTxnDuration](
		ctx, o.db, queryTxnDurationStmt, &m4); err != nil {
		return err
	}
	for _, m := range m4 {
		if !m.duration.Valid {
			m.duration.Float64 = 0
		}
		tidbTxnDurationGauge.WithLabelValues(m.instance, m.opType).Set(m.duration.Float64)
	}

	return nil
}

// NewTiDBObserver creates a new TiDBObserver instance
func NewTiDBObserver(db *sql.DB) *TiDBObserver {
	return &TiDBObserver{
		db: db,
	}
}

type tidbConnIdleDuration struct {
	ts       string
	instance string
	inTxn    int
	duration sql.NullFloat64
}

func (m *tidbConnIdleDuration) columns() []interface{} {
	return []interface{}{&m.ts, &m.instance, &m.inTxn, &m.duration}
}

type tidbConnCount struct {
	ts       string
	instance string
	count    sql.NullInt32
}

func (m *tidbConnCount) columns() []interface{} {
	return []interface{}{&m.ts, &m.instance, &m.count}
}

type tidbQueryDuration struct {
	ts        string
	instance  string
	queryType string
	duration  sql.NullFloat64
}

func (m *tidbQueryDuration) columns() []interface{} {
	return []interface{}{&m.ts, &m.instance, &m.queryType, &m.duration}
}

type tidbTxnDuration struct {
	ts       string
	instance string
	opType   string
	duration sql.NullFloat64
}

func (m *tidbTxnDuration) columns() []interface{} {
	return []interface{}{&m.ts, &m.instance, &m.opType, &m.duration}
}

type metricColumnImpl interface {
	tidbConnIdleDuration | tidbConnCount | tidbQueryDuration | tidbTxnDuration
}

type metricColumnIface[T metricColumnImpl] interface {
	*T
	columns() []interface{}
}

func queryMetrics[T metricColumnImpl, F metricColumnIface[T]](
	ctx context.Context, db *sql.DB, stmt string, metrics *[]F,
) error {
	rows, err := db.QueryContext(ctx, stmt)
	if err != nil {
		return errors.WrapError(errors.ErrMySQLQueryError, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Warn("query metrics close rows failed", zap.Error(err))
		}
	}()
	for rows.Next() {
		var m F = new(T)
		if err := rows.Scan(m.columns()...); err != nil {
			return errors.WrapError(errors.ErrMySQLQueryError, err)
		}
		*metrics = append(*metrics, m)
	}
	return nil
}
