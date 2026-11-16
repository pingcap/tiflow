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

package dbconn

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSlowTransactionSQLDiagnostics(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		for _, parameterized := range []bool{false, true} {
			sql := "UPDATE `exchange`.`t` SET v = 'private-literal' WHERE id = ? LIMIT 1"
			arguments := []any{"private-argument"}
			if parameterized {
				sql = "UPDATE `exchange`.`t` SET v = ? WHERE id = ? LIMIT 1"
				arguments = []any{"private-argument", "private-argument"}
			}
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			sqlConn, err := db.Conn(context.Background())
			require.NoError(t, err)
			mock.ExpectBegin()
			execution := mock.ExpectExec(regexp.QuoteMeta(sql))
			if parameterized {
				execution.WithArgs("private-argument", "private-argument")
			} else {
				execution.WithArgs("private-argument")
			}
			execution.WillDelayFor(1010 * time.Millisecond).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			mock.ExpectClose()
			core, logs := observer.New(zap.WarnLevel)
			ctx := tcontext.NewContext(context.Background(), log.Logger{Logger: zap.New(core)})
			cfg := &config.SubTaskConfig{Name: "task-one", SyncerConfig: config.SyncerConfig{WorkerCount: 1}}
			connection := NewDBConn(cfg, conn.NewBaseConnForTest(sqlConn, nil))
			statements := []string{sql}
			_, err = connection.ExecuteSQLWithIgnore(ctx, nil, nil, statements, arguments)
			require.NoError(t, err)
			require.Equal(t, sql, statements[0], "execute the original SQL, never diagnostic SQL")
			entries := logs.FilterMessage("execute transaction too slow").All()
			require.Len(t, entries, 1)
			fields := entries[0].ContextMap()
			require.Contains(t, fields["query"], "UPDATE")
			require.Contains(t, fields["query"], "`exchange`.`t`")
			require.Contains(t, fields["query"], "LIMIT 1")
			if enabled {
				require.Equal(t, "?", fields["argument"])
				require.NotContains(t, fields["query"], "private-literal")
				if parameterized {
					require.Equal(t, "["+sql+"]", fields["query"])
				}
			} else {
				require.Contains(t, fields["argument"], "private-argument")
				require.Equal(t, "["+sql+"]", fields["query"])
			}
			require.NoError(t, sqlConn.Close())
			require.NoError(t, db.Close())
			require.NoError(t, mock.ExpectationsWereMet())
		}
	}
}
