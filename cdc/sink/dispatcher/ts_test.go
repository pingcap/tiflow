// Copyright 2020 PingCAP, Inc.
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

package dispatcher

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestTsDispatcher(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		row             *model.RowChangedEvent
		exceptPartition int32
	}{
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			CommitTs: 1,
		}, exceptPartition: 1},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			CommitTs: 2,
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			CommitTs: 3,
		}, exceptPartition: 3},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			CommitTs: 1,
		}, exceptPartition: 1},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			CommitTs: 2,
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			CommitTs: 3,
		}, exceptPartition: 3},
	}
	p := &tsDispatcher{partitionNum: 16}
	for _, tc := range testCases {
		require.Equal(t, tc.exceptPartition, p.Dispatch(tc.row))
	}
}
