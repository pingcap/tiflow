package kv

import (
	"context"
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

	cli, err := NewCDCClient(pdCli, nil)
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

// Use etcdSuite for some special reasons, the embed etcd uses zap as the only candidate
// logger and in the logger initializtion it also initializes the grpclog/loggerv2, which
// is not a thread-safe operation and it must be called before any gRPC functions
// ref: https://github.com/grpc/grpc-go/blob/master/grpclog/loggerv2.go#L67-L72
func (s *etcdSuite) TestConnArray(c *check.C) {
	addr := "127.0.0.1:2379"
	ca, err := newConnArray(context.TODO(), 2, addr)
	c.Assert(err, check.IsNil)

	conn1 := ca.Get()
	conn2 := ca.Get()
	c.Assert(conn1, check.Not(check.Equals), conn2)

	conn3 := ca.Get()
	c.Assert(conn1, check.Equals, conn3)

	ca.Close()
}
