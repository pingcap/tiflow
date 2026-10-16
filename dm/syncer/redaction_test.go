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
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/text/encoding"
)

// A failing transformer exercises the conversion-error paths without relying on
// a particular byte sequence being rejected by the Windows-1252 decoder.
type failingRedactionTransformer struct{}

func (failingRedactionTransformer) Reset() {}

func (failingRedactionTransformer) Transform(_, _ []byte, _ bool) (int, int, error) {
	return 0, 0, errors.New("conversion failed")
}

func TestConversionFailureValueRedaction(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	previousDecoder := latin1Decoder
	globalLogger := log.L().Logger
	previousLogger := *globalLogger
	t.Cleanup(func() {
		log.SetRedactLog(previous)
		latin1Decoder = previousDecoder
		*globalLogger = previousLogger
	})
	latin1Decoder = &encoding.Decoder{Transformer: failingRedactionTransformer{}}
	table := mockTableInfo(t, "CREATE TABLE t(v VARCHAR(100)) CHARSET=latin1")
	table.Charset = "latin1"
	table.Columns[0].FieldType.SetCharset("latin1")
	for _, value := range []any{"private-value", []byte("private-value")} {
		for _, enabled := range []bool{false, true} {
			log.SetRedactLog(enabled)
			core, logs := observer.New(zap.DebugLevel)
			*globalLogger = *zap.New(core)
			original := []any{value}
			converted, err := adjustValueFromBinlogData(original, table)
			require.NoError(t, err, "conversion warnings must not change replication behavior")
			require.Len(t, converted, 1)
			require.Equal(t, value, original[0], "logging must not change input data")
			entries := logs.FilterMessage("can't convert latin1 to utf8").All()
			require.Len(t, entries, 1)
			fields := entries[0].ContextMap()
			require.EqualValues(t, len("private-value"), fields["value length"])
			if _, ok := value.(string); ok {
				require.Equal(t, "string", fields["value type"])
			} else {
				require.Equal(t, "bytes", fields["value type"])
			}
			require.Equal(t, "conversion failed", fields["error"])
			if enabled {
				require.Equal(t, "?", fields["value"])
			} else {
				require.Equal(t, "private-value", fields["value"])
			}
		}
	}
}

func TestValidatorQueryArgumentRedaction(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	table := genValidateTableInfo(t, "CREATE TABLE t(id VARCHAR(100) PRIMARY KEY)")
	cond := &Cond{
		TargetTbl: "`test`.`t`", Columns: table.Columns, PK: table.Indices[0],
		PkValues: [][]string{{"private-value"}},
	}
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		queryErr := &mysql.MySQLError{Number: 1062, Message: "Duplicate entry 'private-value' for key 'PRIMARY'"}
		mock.ExpectQuery("SELECT .* FROM .*test.*").WithArgs("private-value").WillReturnError(queryErr)
		mock.ExpectClose()
		core, logs := observer.New(zap.DebugLevel)
		worker := &validateWorker{
			ctx: context.Background(), db: conn.NewBaseDBForTest(db, func() {}),
			L: log.Logger{Logger: zap.New(core)},
		}
		_, err = worker.getTargetRows(cond)
		require.Same(t, queryErr, errors.Cause(err))
		entries := logs.FilterMessage("failed to query").All()
		require.Len(t, entries, 1)
		fields := entries[0].ContextMap()
		require.Contains(t, fields["query"], "SELECT")
		require.Contains(t, fields["query"], "`test`.`t`")
		require.Contains(t, fields["query"], "?")
		require.NotContains(t, fields["query"], "private-value")
		if enabled {
			require.Equal(t, "?", fields["args"])
		} else {
			require.Contains(t, fields["args"], "private-value")
		}
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	}
}
