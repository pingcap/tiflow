package scheduler

import (
	"fmt"
	"testing"

	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type workloadsSuite struct{}

var _ = check.Suite(&workloadsSuite{})

func (s *workloadsSuite) TestWorkloads(c *check.C) {
	w := make(workloads)
	w.SetCapture("capture1", map[int64]uint64{
		1: 1, 2: 2,
	})
	w.SetCapture("capture2", map[int64]uint64{
		4: 1, 3: 2,
	})
	w.SetTable("capture2", 5, 8)
	w.SetTable("capture3", 6, 1)
	w.RemoveTable("capture1", 4)
	w.RemoveTable("capture5", 4)
	w.RemoveTable("capture1", 1)
	c.Assert(w, check.DeepEquals, workloads{
		"capture1": {2: 2},
		"capture2": {4: 1, 3: 2, 5: 8},
		"capture3": {6: 1},
	})
	c.Assert(w.AvgEachTable(), check.Equals, uint64(2+1+2+8+1)/5)
	c.Assert(w.SelectIdleCapture(), check.Equals, "capture3")

	c.Assert(fmt.Sprintf("%.2f%%", w.Skewness()*100), check.Equals, "96.36%")
}
