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
			RowID:  88,
		}, exceptPartition: 8},
	}
	p := &rowIDDispatcher{partitionNum: 16}
	for _, tc := range testCases {
		c.Assert(p.Dispatch(tc.row), check.Equals, tc.exceptPartition)
	}
}
