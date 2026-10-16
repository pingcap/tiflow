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
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCompactorLogRedaction(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	ti, err := createTableInfo(parser.New(), mock.NewContext(), 0,
		"CREATE TABLE t(id INT PRIMARY KEY, value VARCHAR(64))")
	require.NoError(t, err)
	table := &cdcmodel.TableName{Schema: "test", Table: "t"}
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		core, observed := observer.New(zap.DebugLevel)
		c := &compactor{
			logger:             log.Logger{Logger: log.WithRedaction(zap.New(core))},
			keyMap:             make(map[string]map[string]int),
			updateJobMetricsFn: func(bool, string, *job) {},
		}
		before, after := []any{1, "private-old"}, []any{1, "private-new"}
		insert := &job{dml: sqlmodel.NewRowChange(table, nil, nil, before, ti, nil, nil)}
		update := &job{dml: sqlmodel.NewRowChange(table, nil, before, after, ti, nil, nil)}
		c.compactJob(insert)
		c.compactJob(update)
		require.Equal(t, sqlmodel.RowChangeInsert, update.dml.Type())
		require.Equal(t, after, update.dml.GetPostValues())
		for _, message := range []string{"start to compact", "finish to compact"} {
			entries := observed.FilterMessage(message).All()
			require.Len(t, entries, 1)
			data, err := json.Marshal(entries[0].ContextMap())
			require.NoError(t, err)
			if enabled {
				require.NotContains(t, string(data), "private-")
			} else {
				require.Contains(t, string(data), "private-new")
			}
		}
	}
}

func TestValidatorComparisonLogRedaction(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	for _, enabled := range []bool{false, true} {
		log.SetRedactLog(enabled)
		core, observed := observer.New(zap.DebugLevel)
		columnType := types.NewFieldType(mysql.TypeEnum)
		columnType.SetElems([]string{"private-source", "private-target"})
		ctx := &validateCompareContext{
			logger:      log.Logger{Logger: log.WithRedaction(zap.New(core))},
			sourceTable: &cdcmodel.TableName{Schema: "test", Table: "source"},
			targetTable: &cdcmodel.TableName{Schema: "test", Table: "target"},
			columns:     []*model.ColumnInfo{{FieldType: *columnType}},
		}
		equal, err := ctx.compareData("private-key",
			[]*sql.NullString{{String: "private-source", Valid: true}},
			[]*sql.NullString{{String: "private-target", Valid: true}})
		require.NoError(t, err)
		require.False(t, equal, "redaction must not change comparison results")
		entries := observed.FilterMessage("compare failed").All()
		require.Len(t, entries, 1)
		data, err := json.Marshal(entries[0].ContextMap())
		require.NoError(t, err)
		if enabled {
			require.NotContains(t, string(data), "private-")
		} else {
			require.Contains(t, string(data), "private-source")
			require.Contains(t, string(data), "private-target")
			require.Contains(t, string(data), "private-key")
		}
	}
}
