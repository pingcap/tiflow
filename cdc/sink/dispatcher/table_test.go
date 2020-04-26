package dispatcher

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type TableDispatcherSuite struct{}

var _ = check.Suite(&TableDispatcherSuite{})

func (s TableDispatcherSuite) TestTableDispatcher(c *check.C) {
	testCases := []struct {
		row             *model.RowChangedEvent
		exceptPartition int32
	}{
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			CRTs:   1,
		}, exceptPartition: 15},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			CRTs:   2,
		}, exceptPartition: 15},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t1",
			CRTs:   3,
		}, exceptPartition: 15},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			CRTs:   1,
		}, exceptPartition: 5},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			CRTs:   2,
		}, exceptPartition: 5},
		{row: &model.RowChangedEvent{
			Schema: "test",
			Table:  "t2",
			CRTs:   3,
		}, exceptPartition: 5},
	}
	p := &tableDispatcher{partitionNum: 16}
	for _, tc := range testCases {
		c.Assert(p.Dispatch(tc.row), check.Equals, tc.exceptPartition)
	}
}
