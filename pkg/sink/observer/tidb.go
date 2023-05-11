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
	// +----------------------------+------------------+--------+----------+-----------------------+
	// | time                       | instance         | in_txn | quantile | value                 |
	// +----------------------------+------------------+--------+----------+-----------------------+
	// | 2023-01-30 17:42:23.918000 | 10.2.6.127:11080 | 0      |      0.9 |    0.4613936714347638 |
	// | 2023-01-30 17:42:23.918000 | 10.2.6.127:11080 | 1      |      0.9 | 0.0007897614642526763 |
	// | 2023-01-30 17:42:23.918000 | 10.2.6.127:11080 | 0      |     0.99 |    0.5070392371044647 |
	// | 2023-01-30 17:42:23.918000 | 10.2.6.127:11080 | 1      |     0.99 | 0.0026397727272727063 |
	// | 2023-01-30 17:42:23.918000 | 10.2.6.127:11080 | 0      |    0.999 |    0.5116037936714348 |
	// | 2023-01-30 17:42:23.918000 | 10.2.6.127:11080 | 1      |    0.999 |  0.013826666666666192 |
	// +----------------------------+------------------+--------+----------+-----------------------+
	queryConnIdleDurationStmt = `SELECT
	a.time, a.instance, a.in_txn, a.quantile, a.value
	FROM METRICS_SCHEMA.tidb_connection_idle_duration a
	INNER JOIN (
		SELECT instance, in_txn, quantile, MAX(time) time
		FROM METRICS_SCHEMA.tidb_connection_idle_duration
		WHERE quantile in (0.9,0.99,0.999)
		GROUP BY instance, in_txn, quantile
	) b ON a.instance = b.instance AND a.in_txn = b.in_txn AND
	a.quantile = b.quantile AND a.time = b.time AND a.quantile in (0.9,0.99,0.999);`

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

// TiDBObserver is a tidb performance observer. It's not thread-safe.
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
		quantileLabel := strconv.FormatFloat(m.quantile, 'f', -1, 64)
		tidbConnIdleDurationGauge.
			WithLabelValues(m.instance, inTxnLabel, quantileLabel).
			Set(m.duration.Float64)
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

// Close implements Observer
func (o *TiDBObserver) Close() error {
	return o.db.Close()
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
	quantile float64
	duration sql.NullFloat64
}

func (m *tidbConnIdleDuration) columns() []interface{} {
	return []interface{}{&m.ts, &m.instance, &m.inTxn, &m.quantile, &m.duration}
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
