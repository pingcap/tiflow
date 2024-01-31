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

package partition

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestDefaultDispatcher(t *testing.T) {
	t.Parallel()

	tableInfo := &model.TableInfo{
		TableName: model.TableName{
			Schema: "test",
			Table:  "t1",
		},
		IndexColumnsOffset: [][]int{{0}},
	}
	row := &model.RowChangedEvent{
		TableInfo: tableInfo,
		Columns: []*model.Column{
			{
				Name:  "id",
				Value: 1,
				Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			},
		},
	}

	targetPartition, _, err := NewDefaultDispatcher().DispatchRowChangedEvent(row, 3)
	require.NoError(t, err)
	require.Equal(t, int32(0), targetPartition)
}
