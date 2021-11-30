package test_test

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hanfei1991/microcosm/executor"
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/master"
	"github.com/hanfei1991/microcosm/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/test/producer"
	. "github.com/pingcap/check"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
)

var _ = SerialSuites(&testJobSuite{})

type testJobSuite struct{}

func (t *testJobSuite) TestSubmit(c *C) {
	masterCfg := &master.Config{
		Name:              "master1",
		MasterAddr:        "127.0.0.1:1991",
		DataDir:           "/tmp/df",
		KeepAliveTTL:      2 * time.Second,
		KeepAliveInterval: 200 * time.Millisecond,
		RPCTimeout:        time.Second,
	}
	// one master + one executor
	executorCfg := &executor.Config{
		Join:              "127.0.0.1:1991",
		WorkerAddr:        "127.0.0.1:1992",
		KeepAliveTTL:      2 * time.Second,
		KeepAliveInterval: 200 * time.Millisecond,
		RPCTimeout:        time.Second,
	}

	cluster := new(MiniCluster)
	_, err := cluster.CreateMaster(masterCfg)
	c.Assert(err, IsNil)
	executorCtx := cluster.CreateExecutor(executorCfg)
	// Start cluster
	err = cluster.AsyncStartMaster()
	c.Assert(err, IsNil)

	err = cluster.AsyncStartExector()
	c.Assert(err, IsNil)

	time.Sleep(2 * time.Second)

	// test setting
	testServers := []string{"127.0.0.1:9999", "127.0.0.1:9998", "127.0.0.1:9997"}
	tblNum := 10
	recordNum := 10000

	mockServers, err := producer.StartProducerForTest(testServers, int32(tblNum), int32(recordNum))
	defer func() {
		for _, mockServer := range mockServers {
			mockServer.Stop()
		}
	}()
	c.Assert(err, IsNil)
	client, err := master.NewMasterClient(context.Background(), []string{"127.0.0.1:1991"})
	c.Assert(err, IsNil)
	testJobConfig := benchmark.Config{
		Servers:  testServers,
		TableNum: tblNum,
	}
	configBytes, err := json.Marshal(testJobConfig)
	c.Assert(err, IsNil)
	req := &pb.SubmitJobRequest{
		Tp:     pb.SubmitJobRequest_Benchmark,
		Config: configBytes,
	}
	resp, err := client.SubmitJob(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.Err, IsNil)
	tablesCnt := make([]int, tblNum)
	for i := 0; i < tblNum*recordNum; i++ {
		data := executorCtx.RecvRecord()
		r := data.(*runtime.Record)
		log.L().Info("recv record", zap.Int32("table", r.Tid), zap.Int32("pk", r.Payload.(*pb.Record).Pk), zap.Int("ith", i))
		tablesCnt[r.Tid]++
	}
	for _, cnt := range tablesCnt {
		c.Assert(cnt, Equals, recordNum)
	}
}
