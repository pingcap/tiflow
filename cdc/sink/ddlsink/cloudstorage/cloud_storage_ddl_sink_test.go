// Copyright 2022 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestWriteDDLEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s", parentDir)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	sink, err := NewDDLSink(ctx, sinkURI)
	require.Nil(t, err)

	ddlEvent := &model.DDLEvent{
		CommitTs: 100,
		Type:     timodel.ActionAddColumn,
		Query:    "alter table test.table1 add col2 varchar(64)",
		TableInfo: &model.TableInfo{
			Version: 100,
			TableName: model.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 20,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{
						Name:      timodel.NewCIStr("col1"),
						FieldType: *types.NewFieldType(mysql.TypeLong),
					},
					{
						Name:      timodel.NewCIStr("col2"),
						FieldType: *types.NewFieldType(mysql.TypeVarchar),
					},
				},
			},
		},
	}
	tableDir := path.Join(parentDir, "test/table1/meta/")
	err = sink.WriteDDLEvent(ctx, ddlEvent)
	require.Nil(t, err)

	tableSchema, err := os.ReadFile(path.Join(tableDir, "schema_100_4192708364.json"))
	require.Nil(t, err)
	require.JSONEq(t, `{
		"Table": "table1",
		"Schema": "test",
		"Version": 1,
		"TableVersion": 100,
		"Query": "alter table test.table1 add col2 varchar(64)",
		"Type": 5,
		"TableColumns": [
			{
				"ColumnName": "col1",
				"ColumnType": "INT",
				"ColumnPrecision": "11"
			},
			{
				"ColumnName": "col2",
				"ColumnType": "VARCHAR",
				"ColumnPrecision": "5"
			}
		],
		"TableColumnsTotal": 2
	}`, string(tableSchema))
}

func TestWriteCheckpointTs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	parentDir := t.TempDir()
	uri := fmt.Sprintf("file:///%s", parentDir)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	sink, err := NewDDLSink(ctx, sinkURI)
	require.Nil(t, err)
	tables := []*model.TableInfo{
		{
			Version: 100,
			TableName: model.TableName{
				Schema:  "test",
				Table:   "table1",
				TableID: 20,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{
						Name:      timodel.NewCIStr("col1"),
						FieldType: *types.NewFieldType(mysql.TypeLong),
					},
					{
						Name:      timodel.NewCIStr("col2"),
						FieldType: *types.NewFieldType(mysql.TypeVarchar),
					},
				},
			},
		},
	}

	time.Sleep(3 * time.Second)
	err = sink.WriteCheckpointTs(ctx, 100, tables)
	require.Nil(t, err)
	metadata, err := os.ReadFile(path.Join(parentDir, "metadata"))
	require.Nil(t, err)
	require.JSONEq(t, `{"checkpoint-ts":100}`, string(metadata))
}
