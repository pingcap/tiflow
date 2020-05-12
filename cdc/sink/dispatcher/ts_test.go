package dispatcher

import (
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

func Test(t *testing.T) { check.TestingT(t) }

type TsDispatcherSuite struct{}

var _ = check.Suite(&TsDispatcherSuite{})

func (s TsDispatcherSuite) TestTsDispatcher(c *check.C) {
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
		c.Assert(p.Dispatch(tc.row), check.Equals, tc.exceptPartition)
	}
}
