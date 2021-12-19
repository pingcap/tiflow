package test_test

import (
	"context"
	"encoding/json"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor"
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/master"
	"github.com/hanfei1991/microcosm/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/etcdutils"
	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

var _ = SerialSuites(&testJobSuite{})

type testJobSuite struct{}

func (t *testJobSuite) TestSubmit(c *C) {
	masterCfg := &master.Config{
		Etcd: &etcdutils.ConfigParams{
			Name:    "master1",
			DataDir: "/tmp/df",
		},
		MasterAddr:        "127.0.0.1:1991",
		KeepAliveTTL:      20000000 * time.Second,
		KeepAliveInterval: 200 * time.Millisecond,
		RPCTimeout:        time.Second,
	}
	// one master + one executor
	executorCfg := &executor.Config{
		Join:              "127.0.0.1:1991",
		WorkerAddr:        "127.0.0.1:1992",
		KeepAliveTTL:      20000000 * time.Second,
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

	client, err := client.NewMasterClient(context.Background(), []string{"127.0.0.1:1991"})
	c.Assert(err, IsNil)
	testJobConfig := benchmark.Config{
		Servers:      []string{"127.0.0.1:9999", "127.0.0.1:9998", "127.0.0.1:9997"},
		FlowID:       "jobtest",
		TableNum:     10,
		RecordCnt:    10000,
		DDLFrequency: 100,
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
	tablesCnt := make([]int32, testJobConfig.TableNum)
	for i := int32(0); i < testJobConfig.TableNum*testJobConfig.RecordCnt; i++ {
		data := executorCtx.RecvRecord()
		r := data.(*runtime.Record)
		if i%10000 == 0 {
			log.L().Info("recv record", zap.Int32("table", r.Tid), zap.Int32("pk", r.Payload.(*pb.Record).Pk), zap.Int32("ith", i))
		}
		tablesCnt[r.Tid]++
	}
	for _, cnt := range tablesCnt {
		c.Assert(cnt, Equals, testJobConfig.RecordCnt)
	}
	resp1, err := client.CancelJob(context.Background(), &pb.CancelJobRequest{
		JobId: resp.JobId,
	})
	c.Assert(err, IsNil)
	c.Assert(resp1.Err, IsNil)
}
