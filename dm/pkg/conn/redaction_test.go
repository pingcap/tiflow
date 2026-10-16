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

package conn

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSQLFailureLogRedaction(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		sqlConn, err := db.Conn(context.Background())
		require.NoError(t, err)
		core, observed := observer.New(zap.DebugLevel)
		logger := log.Logger{Logger: log.WithRedaction(zap.New(core))}
		ctx := tcontext.NewContext(context.Background(), logger)
		mock.ExpectBegin()
		mock.ExpectExec("INSERT INTO").WithArgs("private-value").WillReturnError(
			&mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'private-value'"})
		mock.ExpectRollback()
		mock.ExpectClose()
		_, err = NewBaseConnForTest(sqlConn, nil).ExecuteSQL(ctx, nil, "task-one",
			[]string{"INSERT INTO t(email) VALUES (?)"}, []any{"private-value"})
		require.ErrorContains(t, err, "private-value", "internal error must retain retry information")
		entries := observed.FilterMessage("execute statement failed").All()
		require.Len(t, entries, 1)
		data, marshalErr := json.Marshal(entries[0].ContextMap())
		require.NoError(t, marshalErr)
		if enabled {
			require.NotContains(t, string(data), "private-value")
			require.Equal(t, "?", entries[0].ContextMap()["argument"])
		} else {
			require.Contains(t, string(data), "private-value")
		}
		require.NoError(t, sqlConn.Close())
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}
}
