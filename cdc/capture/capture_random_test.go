package capture

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/pingcap/check"
)

func Test(t *testing.T) { check.TestingT(t) }

type clusterSuite struct {
}

var _ = check.Suite(&clusterSuite{})

func (s *clusterSuite) TestClusters(c *check.C) {
	ctx := context.Background()
	tester := NewTester(ctx, c, 3)
	tester.CreateChangefeed(ctx, "test-changefeed", tester.CurrentVersion(), math.MaxUint64, "*.*")
	tester.ApplyDDLJob("create database test1")
	tester.ApplyDDLJob("create table test1.test(A int primary key)")
	time.Sleep(10 * time.Second)
}
