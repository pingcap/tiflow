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

package codec

import (
	"testing"

	pmodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	codecv1 "github.com/pingcap/tiflow/cdc/model/codec/v1"
	"github.com/stretchr/testify/require"
)

func TestV1toV2(t *testing.T) {
	var msg1 []byte
	var rv1 *codecv1.RedoLog
	var rv2, rv2_gen *model.RedoLog
	var err error

	rv1 = &codecv1.RedoLog{
		RedoRow: &codecv1.RedoRowChangedEvent{
			Row: &codecv1.RowChangedEvent{
				StartTs:  1,
				CommitTs: 2,
				Table: &codecv1.TableName{
					Schema:      "schema",
					Table:       "table",
					TableID:     1,
					IsPartition: false,
				},
				TableInfo: nil,
				Columns: []*codecv1.Column{
					&codecv1.Column{
						Name: "column",
						Flag: model.BinaryFlag,
					},
				},
				PreColumns: []*codecv1.Column{
					&codecv1.Column{
						Name: "column",
						Flag: model.BinaryFlag,
					},
				},
				IndexColumns: [][]int{[]int{1}},
			},
		},
		RedoDDL: &codecv1.RedoDDLEvent{
			DDL: &codecv1.DDLEvent{
				StartTs:  1,
				CommitTs: 2,
				Type:     pmodel.ActionCreateTable,
			},
		},
	}

	rv2 = &model.RedoLog{
		RedoRow: model.RedoRowChangedEvent{
			Row: &model.RowChangedEvent{
				StartTs:  1,
				CommitTs: 2,
				Table: &model.TableName{
					Schema:      "schema",
					Table:       "table",
					TableID:     1,
					IsPartition: false,
				},
				TableInfo: nil,
				Columns: []*model.Column{
					&model.Column{
						Name: "column",
						Flag: model.BinaryFlag,
					},
				},
				PreColumns: []*model.Column{
					&model.Column{
						Name: "column",
						Flag: model.BinaryFlag,
					},
				},
				IndexColumns: [][]int{[]int{1}},
			},
		},
		RedoDDL: model.RedoDDLEvent{
			DDL: &model.DDLEvent{
				StartTs:  1,
				CommitTs: 2,
				Type:     pmodel.ActionCreateTable,
			},
		},
	}

	// Unmarshal from v1, []byte{} will be transformed into "".
	rv1.RedoRow.Row.Columns[0].Value = []byte{}
	rv1.RedoRow.Row.PreColumns[0].Value = []byte{}
	rv2.RedoRow.Row.Columns[0].Value = ""
	rv2.RedoRow.Row.PreColumns[0].Value = ""

	// Marshal v1 into bytes.
	codecv1.PreMarshal(rv1)
	msg1, err = rv1.MarshalMsg(nil)
	require.Nil(t, err)

	// Unmarshal v2 from v1 bytes.
	rv2_gen, msg1, err = UnmarshalRedoLog(msg1)
	require.Nil(t, err)
	require.Zero(t, len(msg1))
	require.Equal(t, rv2.RedoRow.Row, rv2_gen.RedoRow.Row)

	// For v2, []byte{} will be kept same in marshal and unmarshal.
	rv2.RedoRow.Row.Columns[0].Value = []byte{}
	rv2.RedoRow.Row.PreColumns[0].Value = []byte{}
	rv2_gen.RedoRow.Row.Columns[0].Value = []byte{}
	rv2_gen.RedoRow.Row.PreColumns[0].Value = []byte{}

	msg1, err = MarshalRedoLog(rv2_gen, nil)
	require.Nil(t, err)
	rv2_gen, msg1, err = UnmarshalRedoLog(msg1)
	require.Nil(t, err)
	require.Zero(t, len(msg1))
	require.Equal(t, rv2.RedoRow.Row, rv2_gen.RedoRow.Row)
}
