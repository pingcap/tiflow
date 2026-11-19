// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package applier

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	mysqlParser "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/memquota"
	"github.com/pingcap/tiflow/cdc/redo/reader"
	mysqlDDL "github.com/pingcap/tiflow/cdc/sink/ddlsink/mysql"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink/txn"
	"github.com/pingcap/tiflow/cdc/sink/tablesink"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestApplyTxnOrderTables checks sink configuration and complete ordered transaction replay.
func TestApplyTxnOrderTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	for i := 0; i < 4; i++ {
		mock.ExpectQuery("select tidb_version()").WillReturnError(&mysql.MySQLError{Number: 1305})
	}

	const bigTxnRows = 1000
	for _, bounds := range [][2]int{{1, bigTxnRows}, {bigTxnRows + 1, bigTxnRows + 1}, {bigTxnRows + 2, bigTxnRows + 2}} {
		mock.ExpectBegin()
		for id := bounds[0]; id <= bounds[1]; id++ {
			mock.ExpectExec("REPLACE INTO `test`.`ordered` (`id`) VALUES (?)").WithArgs(id).
				WillReturnResult(sqlmock.NewResult(0, 1))
		}
		mock.ExpectCommit()
	}
	mock.ExpectClose()

	dbFactory := pmysql.NewDBConnectionFactoryForTest()
	dbFactory.SetStandardConnectionFactory(func(context.Context, string) (*sql.DB, error) { return db, nil })
	oldDML, oldDDL, oldReader := txn.GetDBConnImpl, mysqlDDL.GetDBConnImpl, createRedoReader
	txn.GetDBConnImpl, mysqlDDL.GetDBConnImpl = dbFactory, dbFactory
	t.Cleanup(func() {
		txn.GetDBConnImpl, mysqlDDL.GetDBConnImpl, createRedoReader = oldDML, oldDDL, oldReader
	})
	tableInfo := model.BuildTableInfo("test", "ordered", []*model.Column{
		{Name: "id", Type: mysqlParser.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag},
	}, [][]int{{0}})
	rows := make(chan *model.RowChangedEvent, bigTxnRows+2)
	for id := 1; id <= bigTxnRows+2; id++ {
		commitTs := uint64(1200)
		if id > bigTxnRows {
			commitTs += uint64(id - bigTxnRows)
		}
		rows <- &model.RowChangedEvent{
			StartTs: commitTs - 1, CommitTs: commitTs, TableInfo: tableInfo,
			Columns: model.Columns2ColumnDatas([]*model.Column{{Name: "id", Value: id}}, tableInfo),
		}
	}
	close(rows)
	ddls := make(chan *model.DDLEvent)
	close(ddls)
	createRedoReader = func(context.Context, *RedoApplierConfig) (reader.RedoLogReader, error) {
		return NewMockReader(1000, 2000, rows, ddls), nil
	}
	cfg := config.GetDefaultReplicaConfig().Sink
	cfg.TxnAtomicity = util.AddressOf(config.AtomicityLevel("none"))
	cfg.MySQLConfig = &config.MySQLConfig{
		TxnOrderTables: []string{"TEST.ORDERED"}, WorkerCount: util.AddressOf(1), MaxTxnRow: util.AddressOf(64),
		EnableBatchDML: util.AddressOf(false), EnableMultiStatement: util.AddressOf(false),
		EnableCachePreparedStatement: util.AddressOf(false),
	}
	ap := NewRedoApplier(&RedoApplierConfig{
		SinkURI: "mysql://127.0.0.1:4000/?safe-mode=true", SinkConfig: cfg, Dir: t.TempDir(),
		CaseSensitive: false,
	})
	require.NoError(t, ap.Apply(ctx))
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestApplyTxnOrderPartition checks replay validation for matching partitioned tables.
func TestApplyTxnOrderPartition(t *testing.T) {
	cfg := &config.SinkConfig{MySQLConfig: &config.MySQLConfig{TxnOrderTables: []string{"test.ordered"}}}
	f, err := cfg.GetTxnOrderFilter(true)
	require.NoError(t, err)
	ap := NewRedoApplier(&RedoApplierConfig{})
	ap.txnOrderFilter = f
	tableInfo := model.BuildTableInfo("test", "ordered", []*model.Column{
		{Name: "id", Type: mysqlParser.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag},
	}, [][]int{{0}})
	tableInfo.TableName.IsPartition = true
	err = ap.applyRow(&model.RowChangedEvent{TableInfo: tableInfo, CommitTs: 1100}, 1000)
	require.ErrorContains(t, err, "does not support partitioned table")
	for _, ddlType := range []timodel.ActionType{timodel.ActionCreateTable, timodel.ActionRenameTable, timodel.ActionAlterTablePartitioning} {
		err := ap.applyDDL(context.Background(), &model.DDLEvent{TableInfo: tableInfo, Type: ddlType, CommitTs: 1100}, 1000)
		require.ErrorContains(t, err, "does not support partitioned table")
	}
	tableInfo.TableName.Table = "unordered"
	ordered, err := ap.isTxnOrderTable(tableInfo)
	require.NoError(t, err)
	require.False(t, ordered)
}

type replayTxnSink struct {
	rowCounts []int
}

// WriteEvents records flushed transaction sizes and immediately acknowledges them.
func (s *replayTxnSink) WriteEvents(events ...*dmlsink.TxnCallbackableEvent) error {
	for _, event := range events {
		s.rowCounts = append(s.rowCounts, len(event.Event.Rows))
		event.Callback()
	}
	return nil
}

// SchemeOption uses the MySQL transaction format for replay tests.
func (s *replayTxnSink) SchemeOption() (string, bool) { return "mysql", false }

// Close implements the event sink interface.
func (s *replayTxnSink) Close() {}

// Dead reports that this synchronous test sink remains healthy.
func (s *replayTxnSink) Dead() <-chan struct{} { return nil }

// TestApplyRowTxnAtomicity checks quota pressure, ordered overrides, and unmatched tables.
func TestApplyRowTxnAtomicity(t *testing.T) {
	for _, tc := range []struct {
		name     string
		ordered  bool
		splitTxn bool
	}{
		{"ordered table", true, true},
		{"unmatched table", false, true},
		{"table atomicity", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const tableID, rowCount, quota = 1, 1000, 128 * 1024
			ap := NewRedoApplier(&RedoApplierConfig{})
			ap.splitTxn = tc.splitTxn
			ap.tableTxnAtomicity = make(map[model.TableID]bool)
			ap.tableResolvedTsMap = make(map[model.TableID]*memquota.MemConsumeRecord)
			ap.memQuota = memquota.NewMemQuota(model.DefaultChangeFeedID(applierChangefeed), quota, "test")
			defer ap.memQuota.Close()
			cfg := &config.SinkConfig{MySQLConfig: &config.MySQLConfig{TxnOrderTables: []string{"test.ordered"}}}
			var err error
			ap.txnOrderFilter, err = cfg.GetTxnOrderFilter(true)
			require.NoError(t, err)
			tableName := "ordinary"
			if tc.ordered {
				tableName = "ordered"
			}
			tableInfo := model.BuildTableInfo("test", tableName, []*model.Column{
				{Name: "id", Type: mysqlParser.TypeLong, Flag: model.HandleKeyFlag | model.PrimaryKeyFlag},
			}, [][]int{{0}})
			backend := &replayTxnSink{}
			tableSink := tablesink.New(model.DefaultChangeFeedID(applierChangefeed), spanz.TableIDToComparableSpan(tableID),
				1000, backend, &dmlsink.TxnEventAppender{}, pdutil.NewClock4Test(),
				prometheus.NewCounter(prometheus.CounterOpts{}), prometheus.NewHistogram(prometheus.HistogramOpts{}))
			ap.tableSinks = map[model.TableID]tablesink.TableSink{tableID: tableSink}
			defer tableSink.Close()
			for id := 1; id <= rowCount; id++ {
				require.NoError(t, ap.applyRow(&model.RowChangedEvent{
					PhysicalTableID: tableID, StartTs: 1100, CommitTs: 1200, TableInfo: tableInfo,
					Columns: model.Columns2ColumnDatas([]*model.Column{{Name: "id", Value: id}}, tableInfo),
				}, 1000))
			}
			atomic := tc.ordered || !tc.splitTxn
			if atomic {
				require.Empty(t, backend.rowCounts)
				require.Greater(t, ap.memQuota.GetUsedBytes(), uint64(quota))
			} else {
				require.NotEmpty(t, backend.rowCounts)
				require.LessOrEqual(t, ap.memQuota.GetUsedBytes(), uint64(quota))
			}
			require.NoError(t, ap.waitTableFlush(context.Background(), tableID, 1200))
			if atomic {
				require.Equal(t, []int{rowCount}, backend.rowCounts)
			} else {
				require.Greater(t, len(backend.rowCounts), 1)
				var total int
				for _, count := range backend.rowCounts {
					total += count
				}
				require.Equal(t, rowCount, total)
			}
		})
	}
}
