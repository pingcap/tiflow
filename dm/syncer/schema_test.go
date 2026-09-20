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

package syncer

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/filter"
	router "github.com/pingcap/tidb/pkg/util/table-router"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/schema"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/dm/syncer/dbconn"
	"github.com/stretchr/testify/require"
)

func TestOperateSchemaSetFullText(t *testing.T) {
	for _, from := range []string{"statement", "source", "target"} {
		t.Run(from, func(t *testing.T) {
			ctx := context.Background()
			cfg := genDefaultSubTaskConfig4Test()
			cfg.RouteRules = []*router.TableRule{{
				SchemaPattern: "db", TablePattern: "t",
				TargetSchema: "target_db", TargetTable: "target_t",
			}}
			syncer := NewSyncer(cfg, nil, nil)
			syncer.exprFilterGroup = NewExprFilterGroup(tcontext.Background(), utils.NewSessionCtx(nil), nil)
			require.NoError(t, syncer.genRouter())

			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			syncer.fromDB = &dbconn.UpStreamConn{BaseDB: conn.NewBaseDBForTest(db)}
			checkPointDB, checkPointMock, err := sqlmock.New()
			require.NoError(t, err)
			defer checkPointDB.Close()
			checkPointConn, err := checkPointDB.Conn(ctx)
			require.NoError(t, err)
			syncer.checkpoint.(*RemoteCheckPoint).dbConn = dbconn.NewDBConn(cfg,
				conn.NewBaseConnForTest(checkPointConn, &retry.FiniteRetryStrategy{}))

			createSQL := "CREATE TABLE target_t (id VARCHAR(14) PRIMARY KEY, text_col TEXT, " +
				"normal_col INT, KEY normal_idx(normal_col), FULLTEXT INDEX ft_idx(text_col) WITH PARSER STANDARD)"
			req := &pb.OperateWorkerSchemaRequest{
				Op: pb.SchemaOp_SetSchema, Database: "db", Table: "t",
				Schema: createSQL, FromSource: from == "source", FromTarget: from == "target",
			}
			if from != "statement" {
				showDB, showMock, err := sqlmock.New()
				require.NoError(t, err)
				defer showDB.Close()
				showConn, err := showDB.Conn(ctx)
				require.NoError(t, err)
				dbConn := dbconn.NewDBConn(cfg, conn.NewBaseConnForTest(showConn, nil))
				tableID := "`db`.`t`"
				if from == "source" {
					syncer.fromConn = dbConn
				} else {
					syncer.downstreamTrackConn = dbConn
					tableID = "`target_db`.`target_t`"
				}
				req.Schema = "" // The fetched schema, not caller-supplied SQL, must be used.
				showMock.ExpectQuery(regexp.QuoteMeta("SHOW CREATE TABLE " + tableID)).WillReturnRows(
					sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("target_t", createSQL))
				t.Cleanup(func() { require.NoError(t, showMock.ExpectationsWereMet()) })
			}

			mock.ExpectQuery("SHOW VARIABLES LIKE 'sql_mode'").WillReturnRows(
				sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("sql_mode", ""))
			checkPointMock.ExpectBegin()
			checkPointMock.ExpectExec(".*INSERT INTO .* VALUES.* ON DUPLICATE KEY UPDATE.*").
				WillReturnResult(sqlmock.NewResult(0, 1))
			checkPointMock.ExpectCommit()

			_, err = syncer.OperateSchema(ctx, req)
			require.NoError(t, err)
			ti := syncer.checkpoint.GetTableInfo("db", "t")
			require.NotNil(t, ti)
			require.Equal(t, "t", ti.Name.O)
			require.Len(t, ti.Indices, 3)
			require.True(t, ti.FindIndexByName("primary").Unique)
			require.NotNil(t, ti.FindIndexByName("normal_idx"))
			idx := ti.FindIndexByName("ft_idx")
			require.NotNil(t, idx)
			require.False(t, idx.Unique)
			require.Equal(t, model.ColumnarIndexTypeNA, idx.GetColumnarIndexType())
			require.Equal(t, 1, idx.Columns[0].Length)
			require.Equal(t, createSQL, req.Schema)

			// The checkpoint representation can bootstrap the tracker on resume.
			syncer.schemaTracker, err = schema.NewTestTracker(ctx, cfg.Name, nil, syncer.tctx.L())
			require.NoError(t, err)
			defer syncer.schemaTracker.Close()
			require.NoError(t, syncer.schemaTracker.CreateSchemaIfNotExists("db"))
			table := &filter.Table{Schema: "db", Name: "t"}
			require.NoError(t, syncer.schemaTracker.CreateTableIfNotExists(table, ti))
			tracked, err := syncer.schemaTracker.GetTableInfo(table)
			require.NoError(t, err)
			require.Equal(t, idx, tracked.FindIndexByName("ft_idx"))
			require.NoError(t, mock.ExpectationsWereMet())
			require.NoError(t, checkPointMock.ExpectationsWereMet())
		})
	}
}
