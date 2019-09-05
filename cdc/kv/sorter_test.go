package kv

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/cdcpb"
)

type SorterSuite struct{}

var _ = check.Suite(&SorterSuite{})

func testSorter(c *check.C, items []sortItem, expectMaxCommitTS []uint64) {
	var maxCommitTS []uint64
	var maxTS uint64

	var lastGetSortItemTS uint64
	sorter := newSorter(func(item sortItem) {
		maxCommitTS = append(maxCommitTS, item.commit)
		atomic.StoreUint64(&lastGetSortItemTS, item.commit)
	})

	for _, item := range items {
		sorter.pushTSItem(item)

		// we should never push item with commit ts less than lastGetSortItemTS, or something go wrong
		if item.tp == cdcpb.Event_COMMIT {
			c.Assert(item.commit, check.Greater, atomic.LoadUint64(&lastGetSortItemTS))
		} else if item.tp == cdcpb.Event_PREWRITE {
			c.Assert(sorter.allMatched(), check.IsFalse)
		}

		if item.commit > maxTS {
			maxTS = item.commit
		}
	}

	time.Sleep(time.Millisecond * 50)
	sorter.close()

	c.Logf("testSorter items: %v, get maxCommitTS: %v", items, maxCommitTS)
	c.Logf("items num: %d, get sort item num: %d", len(items), len(maxCommitTS))

	if expectMaxCommitTS != nil {
		c.Assert(maxCommitTS, check.DeepEquals, expectMaxCommitTS)
	}
	for i := 1; i < len(maxCommitTS); i++ {
		c.Assert(maxCommitTS[i], check.Greater, maxCommitTS[i-1])
	}
	c.Assert(maxTS, check.Equals, maxCommitTS[len(maxCommitTS)-1])
	c.Assert(sorter.allMatched(), check.IsTrue)
}

func (s *SorterSuite) TestSorter(c *check.C) {
	var items = []sortItem{
		{
			start: 1,
			tp:    cdcpb.Event_PREWRITE,
		},
		{
			start:  1,
			commit: 2,
			tp:     cdcpb.Event_COMMIT,
		},
	}
	testSorter(c, items, []uint64{2})

	items = []sortItem{
		{
			start: 1,
			tp:    cdcpb.Event_PREWRITE,
		},
		{
			start: 2,
			tp:    cdcpb.Event_PREWRITE,
		},
		{
			start:  2,
			commit: 3,
			tp:     cdcpb.Event_COMMIT,
		},
		{
			start:  1,
			commit: 10,
			tp:     cdcpb.Event_COMMIT,
		},
	}
	testSorter(c, items, []uint64{3, 10})

	items = []sortItem{
		{
			start: 1,
			tp:    cdcpb.Event_PREWRITE,
		},
		{
			start: 2,
			tp:    cdcpb.Event_PREWRITE,
		},
		{
			start:  1,
			commit: 10,
			tp:     cdcpb.Event_COMMIT,
		},
		{
			start:  2,
			commit: 9,
			tp:     cdcpb.Event_COMMIT,
		},
	}
	testSorter(c, items, []uint64{10})

	items = append(items, []sortItem{
		{
			start: 20,
			tp:    cdcpb.Event_PREWRITE,
		},
		{
			start:  20,
			commit: 200,
			tp:     cdcpb.Event_COMMIT,
		},
	}...)
	testSorter(c, items, []uint64{10, 200})

	// test random data
	items = items[:0]
	for item := range newItemGenerator(500, 10, 0) {
		items = append(items, item)
	}
	testSorter(c, items, nil)

	items = items[:0]
	for item := range newItemGenerator(5000, 1, 0) {
		items = append(items, item)
	}
	testSorter(c, items, nil)

	items = items[:0]
	for item := range newItemGenerator(500, 10, 10) {
		items = append(items, item)
	}
	testSorter(c, items, nil)

	items = items[:0]
	for item := range newItemGenerator(5000, 1, 10) {
		items = append(items, item)
	}
	testSorter(c, items, nil)
}
