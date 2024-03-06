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

package partition

import (
	"testing"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestColumnsDispatcher(t *testing.T) {
	t.Parallel()

	tidbTableInfo := &timodel.TableInfo{
		ID:   100,
		Name: timodel.NewCIStr("t1"),
		Columns: []*timodel.ColumnInfo{
			{ID: 1, Name: timodel.NewCIStr("col2"), Offset: 1, FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 2, Name: timodel.NewCIStr("col1"), Offset: 0, FieldType: *types.NewFieldType(mysql.TypeLong)},
			{ID: 3, Name: timodel.NewCIStr("col3"), Offset: 2, FieldType: *types.NewFieldType(mysql.TypeLong)},
		},
	}
	tableInfo := model.WrapTableInfo(100, "test", 33, tidbTableInfo)
	event := &model.RowChangedEvent{
		TableInfo: tableInfo,
		Columns: []*model.ColumnData{
			{ColumnID: 1, Value: 11},
			{ColumnID: 2, Value: 22},
			{ColumnID: 3, Value: 33},
		},
	}

	p := NewColumnsDispatcher([]string{"col-2", "col-not-found"})
	_, _, err := p.DispatchRowChangedEvent(event, 16)
	require.ErrorIs(t, err, errors.ErrDispatcherFailed)

	p = NewColumnsDispatcher([]string{"col2", "col1"})
	index, _, err := p.DispatchRowChangedEvent(event, 16)
	require.NoError(t, err)
	require.Equal(t, int32(15), index)
}
