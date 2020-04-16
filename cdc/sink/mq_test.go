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

package sink

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb-tools/pkg/filter"
)

type PartitionerSuite struct{}

var _ = check.Suite(&PartitionerSuite{})

func (s PartitionerSuite) TestPartitionerDefaultRule(c *check.C) {
	testCases := []struct {
		row             *model.RowChangedEvent
		exceptPartition int32
	}{
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t1",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 1,
				},
			},
		}, exceptPartition: 15},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t1",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 2,
				},
			},
		}, exceptPartition: 5},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t1",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 3,
				},
			},
		}, exceptPartition: 3},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t2",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 1,
				},
			},
		}, exceptPartition: 12},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t2",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 2,
				},
			},
		}, exceptPartition: 6},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t2",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 3,
				},
			},
		}, exceptPartition: 0},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t3",
			Columns: map[string]*model.Column{
				"id": {
					Value: 1,
				},
			},
		}, exceptPartition: 3},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t3",
			Columns: map[string]*model.Column{
				"id": {
					Value: 2,
				},
			},
		}, exceptPartition: 3},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t3",
			Columns: map[string]*model.Column{
				"id": {
					Value: 3,
				},
			},
		}, exceptPartition: 3},
	}
	p := newPartitioner(&util.ReplicaConfig{}, 16)
	for _, tc := range testCases {
		c.Assert(p.calPartition(tc.row), check.Equals, tc.exceptPartition)
	}
}

func (s PartitionerSuite) TestPartitionerRowIDRule(c *check.C) {
	testCases := []struct {
		row             *model.RowChangedEvent
		exceptPartition int32
	}{
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			RowID:  1,
		}, exceptPartition: 1},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			RowID:  2,
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			RowID:  3,
		}, exceptPartition: 3},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			RowID:  1,
		}, exceptPartition: 1},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			RowID:  2,
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			RowID:  3,
		}, exceptPartition: 3},
	}
	p := newPartitioner(&util.ReplicaConfig{
		FilterCaseSensitive: true,
		SinkPartitionRules: []*util.PartitionRule{
			{
				Table: filter.Table{Schema: "test", Name: "t1"},
				Rule:  "rowid",
			}, {
				Table: filter.Table{Schema: "test", Name: "t2"},
				Rule:  "rowID",
			},
		},
	}, 16)
	for _, tc := range testCases {
		c.Assert(p.calPartition(tc.row), check.Equals, tc.exceptPartition)
	}
}

func (s PartitionerSuite) TestPartitionerTsRule(c *check.C) {
	testCases := []struct {
		row             *model.RowChangedEvent
		exceptPartition int32
	}{
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			Ts:     1,
		}, exceptPartition: 1},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			Ts:     2,
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			Ts:     3,
		}, exceptPartition: 3},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			Ts:     1,
		}, exceptPartition: 1},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			Ts:     2,
		}, exceptPartition: 2},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			Ts:     3,
		}, exceptPartition: 3},
	}
	p := newPartitioner(&util.ReplicaConfig{
		FilterCaseSensitive: false,
		SinkPartitionRules: []*util.PartitionRule{
			{
				Table: filter.Table{Schema: "test", Name: "t1"},
				Rule:  "ts",
			}, {
				Table: filter.Table{Schema: "test", Name: "T2"},
				Rule:  "TS",
			},
		},
	}, 16)
	for _, tc := range testCases {
		c.Assert(p.calPartition(tc.row), check.Equals, tc.exceptPartition)
	}
}

func (s PartitionerSuite) TestPartitionerTableRule(c *check.C) {
	testCases := []struct {
		row             *model.RowChangedEvent
		exceptPartition int32
	}{
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			Ts:     1,
		}, exceptPartition: 15},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			Ts:     2,
		}, exceptPartition: 15},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			Ts:     3,
		}, exceptPartition: 15},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			Ts:     1,
		}, exceptPartition: 5},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			Ts:     2,
		}, exceptPartition: 5},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			Ts:     3,
		}, exceptPartition: 5},
	}
	p := newPartitioner(&util.ReplicaConfig{
		FilterCaseSensitive: false,
		SinkPartitionRules: []*util.PartitionRule{
			{
				Table: filter.Table{Schema: "test", Name: "t1"},
				Rule:  "table",
			}, {
				Table: filter.Table{Schema: "test", Name: "T2"},
				Rule:  "TaBlE",
			},
		},
	}, 16)
	for _, tc := range testCases {
		c.Assert(p.calPartition(tc.row), check.Equals, tc.exceptPartition)
	}
}
