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

	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	codecv1 "github.com/pingcap/tiflow/cdc/model/codec/v1"
	"github.com/stretchr/testify/require"
)

func TestV1toV2(t *testing.T) {
	var msg1, msg2 []byte
	var rv1 *codecv1.RedoLog
	var rv2, rv2_gen *model.RedoLog
	var err error

	rv1 = &codecv1.RedoLog{
		RedoRow: &codecv1.RedoRowChangedEvent{
			Row: &codecv1.RowChangedEvent{
				StartTs:  1,
				CommitTs: 2,
				RowID:    1,
				Table: &codecv1.TableName{
					Schema:      "schema",
					Table:       "table",
					TableID:     1,
					IsPartition: false,
				},
				ColInfos: []rowcodec.ColInfo{
					rowcodec.ColInfo{
						ID:            1,
						IsPKHandle:    true,
						VirtualGenCol: false,
						Ft:            nil,
					},
				},
				TableInfo: nil,
				Columns: []*codecv1.Column{
					&codecv1.Column{
						Name:    "column",
						Value:   1,
						Default: 0,
					},
				},
				PreColumns: []*codecv1.Column{
					&codecv1.Column{
						Name:    "column",
						Value:   1,
						Default: 0,
					},
				},
				IndexColumns: [][]int{[]int{1}},
			},
		},
	}

	rv2 = &model.RedoLog{
		RedoRow: &model.RowChangedEvent{
			StartTs:  1,
			CommitTs: 2,
			RowID:    1,
			Table: &model.TableName{
				Schema:      "schema",
				Table:       "table",
				TableID:     1,
				IsPartition: false,
			},
			ColInfos: []rowcodec.ColInfo{
				rowcodec.ColInfo{
					ID:            1,
					IsPKHandle:    true,
					VirtualGenCol: false,
					Ft:            nil,
				},
			},
			TableInfo: nil,
			Columns: []*model.Column{
				&model.Column{
					Name:    "column",
					Value:   1,
					Default: 0,
				},
			},
			PreColumns: []*model.Column{
				&model.Column{
					Name:    "column",
					Value:   1,
					Default: 0,
				},
			},
			IndexColumns: [][]int{[]int{1}},
		},
	}

	// Unmarshal into v2 from v1.
	msg1, err = rv1.MarshalMsg(nil)
	require.Nil(t, err)
	rv2_gen, msg1, err = UnmarshalRedoLog(msg1)
	require.Nil(t, err)
	require.Zero(t, len(msg1))

	// rv2_gen should equal to rv2.
	msg1, err = MarshalRedoLog(rv2_gen, nil)
	require.Nil(t, err)
	msg2, err = MarshalRedoLog(rv2, nil)
	require.Nil(t, err)
	require.Equal(t, msg1, msg2)

	// Unmarshal and marshal v2.
	rv2_gen, _, err = UnmarshalRedoLog(msg2)
	require.Nil(t, err)
	msg1, err = MarshalRedoLog(rv2_gen, nil)
	require.Nil(t, err)
	require.Equal(t, msg1, msg2)
}
