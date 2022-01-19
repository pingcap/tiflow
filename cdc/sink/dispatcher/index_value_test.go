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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type IndexValueDispatcherSuite struct{}

var _ = check.Suite(&IndexValueDispatcherSuite{})

func (s IndexValueDispatcherSuite) TestIndexValueDispatcher(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		row             *model.RowChangedEvent
		exceptPartition int32
	}{
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 22,
					Flag:  0,
				},
			},
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 22,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 22,
					Flag:  0,
				},
			},
		}, exceptPartition: 11},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t1",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 33,
					Flag:  0,
				},
			},
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 22,
					Flag:  model.HandleKeyFlag,
				},
			},
		}, exceptPartition: 5},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "b",
					Value: 22,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				},
			},
		}, exceptPartition: 5},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 0,
					Flag:  model.HandleKeyFlag,
				},
			},
		}, exceptPartition: 14},
		{row: &model.RowChangedEvent{
			Table: &model.TableName{
				Schema: "test",
				Table:  "t2",
			},
			Columns: []*model.Column{
				{
					Name:  "a",
					Value: 11,
					Flag:  model.HandleKeyFlag,
				}, {
					Name:  "b",
					Value: 33,
					Flag:  model.HandleKeyFlag,
				},
			},
		}, exceptPartition: 2},
	}
	p := newIndexValueDispatcher(16)
	for _, tc := range testCases {
		c.Assert(p.Dispatch(tc.row), check.Equals, tc.exceptPartition)
	}
}
