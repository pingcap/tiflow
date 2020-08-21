package sink

import (
	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
)

type TxnsHeapSuite struct{}

var _ = check.Suite(&TxnsHeapSuite{})

func (s TxnsHeapSuite) TestTxnsHeap(c *check.C) {
	testCases := []struct {
		txnsMap  map[model.TableID][]*model.SingleTableTxn
		expected []*model.SingleTableTxn
	}{{
		txnsMap:  nil,
		expected: nil,
	}, {
		txnsMap: map[model.TableID][]*model.SingleTableTxn{
			1: {
				{CommitTs: 1}, {CommitTs: 3}, {CommitTs: 5}, {CommitTs: 7}, {CommitTs: 9},
			},
			2: {
				{CommitTs: 1}, {CommitTs: 10}, {CommitTs: 15}, {CommitTs: 15}, {CommitTs: 15},
			},
			3: {
				{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}, {CommitTs: 2}, {CommitTs: 3},
			},
		},
		expected: []*model.SingleTableTxn{{CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}, {CommitTs: 1}, {CommitTs: 2},
			{CommitTs: 3}, {CommitTs: 3}, {CommitTs: 5}, {CommitTs: 7}, {CommitTs: 9}, {CommitTs: 10}, {CommitTs: 15}, {CommitTs: 15}, {CommitTs: 15}},
	}}

	for _, tc := range testCases {
		h := newTxnsHeap(tc.txnsMap)
		i := 0
		h.iter(func(txn *model.SingleTableTxn) bool {
			c.Assert(txn, check.DeepEquals, tc.expected[i])
			i++
			return true
		})
	}
}
