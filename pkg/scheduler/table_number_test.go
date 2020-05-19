package scheduler

import (
	"fmt"

	"github.com/pingcap/check"
)

type tableNumberSuite struct{}

var _ = check.Suite(&tableNumberSuite{})

func (s *tableNumberSuite) TestTableNumberScheduler(c *check.C) {
	scheduler := NewTableNumberScheduler()
	scheduler.ResetWorkloads("capture1", map[int64]uint64{1: 1, 2: 1})
	scheduler.ResetWorkloads("capture2", map[int64]uint64{3: 1, 4: 1})
	scheduler.ResetWorkloads("capture3", map[int64]uint64{5: 1, 6: 1, 7: 1, 8: 1})
	c.Assert(fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), check.Equals, "35.36%")
	c.Assert(len(scheduler.DistributeTables([]int64{10, 11, 12, 13, 14, 15, 16, 17})), check.Equals, 8)
	c.Assert(fmt.Sprintf("%.2f%%", scheduler.Skewness()*100), check.Equals, "8.84%")
	scheduler.ResetWorkloads("capture1", map[int64]uint64{})

	//TODO add more tests
}
