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
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type RowIDDispatcherSuite struct{}

var _ = check.Suite(&RowIDDispatcherSuite{})

func (s RowIDDispatcherSuite) TestRowIDDispatcher(c *check.C) {
	testCases := []struct {
		row             *model.RowChangedEvent
		exceptPartition int32
	}{
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			RowID: 1,
		}, exceptPartition: 1},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			RowID: 2,
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			RowID: 3,
		}, exceptPartition: 3},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			RowID: 1,
		}, exceptPartition: 1},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			RowID: 2,
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			RowID: 88,
		}, exceptPartition: 8},
	}
	p := &rowIDDispatcher{partitionNum: 16}
	for _, tc := range testCases {
		c.Assert(p.Dispatch(tc.row), check.Equals, tc.exceptPartition)
	}
}
