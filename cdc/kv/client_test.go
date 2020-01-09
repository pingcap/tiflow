package kv

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
)

func Test(t *testing.T) { check.TestingT(t) }

type clientSuite struct {
}

var _ = check.Suite(&clientSuite{})

func (s *clientSuite) TestNewClose(c *check.C) {
	cluster := mocktikv.NewCluster()
	pdCli := mocktikv.NewPDClient(cluster)

	cli, err := NewCDCClient(pdCli)
	c.Assert(err, check.IsNil)

	err = cli.Close()
	c.Assert(err, check.IsNil)
}

func (s *clientSuite) TestUpdateCheckpointTS(c *check.C) {
	var checkpointTS uint64
	var g sync.WaitGroup
	maxValueCh := make(chan uint64, 64)
	update := func() uint64 {
		var maxValue uint64
		for i := 0; i < 1024; i++ {
			value := rand.Uint64()
			if value > maxValue {
				maxValue = value
			}
			updateCheckpointTS(&checkpointTS, value)
		}
		return maxValue
	}
	for i := 0; i < 64; i++ {
		g.Add(1)
		go func() {
			maxValueCh <- update()
			g.Done()
		}()
	}
	g.Wait()
	close(maxValueCh)
	var maxValue uint64
	for v := range maxValueCh {
		if maxValue < v {
			maxValue = v
		}
	}
	c.Assert(checkpointTS, check.Equals, maxValue)
}
