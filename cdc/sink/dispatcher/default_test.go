package dispatcher

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type DefaultDispatcherSuite struct{}

var _ = check.Suite(&DefaultDispatcherSuite{})

func (s DefaultDispatcherSuite) TestDefaultDispatcher(c *check.C) {
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
		}, exceptPartition: 7},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t1",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 2,
				},
			},
		}, exceptPartition: 13},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t1",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 3,
				},
			},
		}, exceptPartition: 11},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t2",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 1,
				},
			},
		}, exceptPartition: 7},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t2",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 2,
				},
			},
		}, exceptPartition: 13},
		{row: &model.RowChangedEvent{
			Schema:       "test",
			Table:        "t2",
			IndieMarkCol: "id",
			Columns: map[string]*model.Column{
				"id": {
					Value: 3,
				},
			},
		}, exceptPartition: 11},
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
	p := &defaultDispatcher{partitionNum: 16}
	for _, tc := range testCases {
		c.Assert(p.Dispatch(tc.row), check.Equals, tc.exceptPartition)
	}
}
