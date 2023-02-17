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

	testCases := []struct {
		row             *model.RowChangedEvent
		expectPartition int32
	}{
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 1,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				},
			},
			IndexColumns: [][]int{{0}},
		}, expectPartition: 11},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 2,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				},
			},
			IndexColumns: [][]int{{0}},
		}, expectPartition: 1},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 3,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				},
			},
			IndexColumns: [][]int{{0}},
		}, expectPartition: 7},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 1,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				}, {
					Name:  "a",
					Value: 1,
				},
			},
			IndexColumns: [][]int{{0}},
		}, expectPartition: 1},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 2,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				}, {
					Name:  "a",
					Value: 2,
				},
			},
			IndexColumns: [][]int{{0}},
		}, expectPartition: 11},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 3,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				}, {
					Name:  "a",
					Value: 3,
				},
			},
			IndexColumns: [][]int{{0}},
		}, expectPartition: 13},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 3,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				}, {
					Name:  "a",
					Value: 4,
				},
			},
			IndexColumns: [][]int{{0}},
		}, expectPartition: 13},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t3",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 1,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				},
				{
					Name:  "a",
					Value: 2,
					Flag:  model.UniqueKeyFlag,
				},
			},
			IndexColumns: [][]int{{0}, {1}},
		}, expectPartition: 3},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t3",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 2,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				}, {
					Name:  "a",
					Value: 3,
					Flag:  model.UniqueKeyFlag,
				},
			},
			IndexColumns: [][]int{{0}, {1}},
		}, expectPartition: 3},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t3",
			},
			Columns: []*model.Column{
				{
					Name:  "id",
					Value: 3,
					Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
				}, {
					Name:  "a",
					Value: 4,
					Flag:  model.UniqueKeyFlag,
				},
			},
			IndexColumns: [][]int{{0}, {1}},
		}, expectPartition: 3},
	}
	p := NewDefaultDispatcher(false)
	for _, tc := range testCases {
		require.Equal(t, tc.expectPartition, p.DispatchRowChangedEvent(tc.row, 16))
	}
}

func TestDefaultDispatcherWithOldValue(t *testing.T) {
	t.Parallel()

	row := &model.RowChangedEvent{
		Table: &model.TableName{
			Schema: "test",
			Table:  "t3",
		},
		Columns: []*model.Column{
			{
				Name:  "id",
				Value: 2,
				Flag:  model.HandleKeyFlag | model.PrimaryKeyFlag,
			}, {
				Name:  "a",
				Value: 3,
				Flag:  model.UniqueKeyFlag,
			},
		},
		IndexColumns: [][]int{{0}, {1}},
	}

	p := NewDefaultDispatcher(true)
	require.Equal(t, int32(3), p.DispatchRowChangedEvent(row, 16))
}
