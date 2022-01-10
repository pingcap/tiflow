package test_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/jobmaster/benchmark"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/phayes/freeport"
	. "github.com/pingcap/check"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

var _ = SerialSuites(&testJobSuite{})

type testJobSuite struct{}

func (t *testJobSuite) TestSubmit(c *C) {
	cluster := NewEmptyMiniCluster()
	masterAddr, _, masterCtx, executorCtx := cluster.Start1M1E(c)
	client, err := client.NewMasterClient(context.Background(), []string{masterAddr})
	c.Assert(err, IsNil)
	testJobConfig := benchmark.Config{
		Servers:      getBenchmarkServers(3, c),
		FlowID:       "jobtest",
		TableNum:     10,
		RecordCnt:    10000,
		DDLFrequency: 100,
	}
	configBytes, err := json.Marshal(testJobConfig)
	c.Assert(err, IsNil)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_Benchmark,
		Config: configBytes,
	}
	resp, err := client.SubmitJob(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.Err, IsNil)
	tablesCnt := make([]int32, testJobConfig.TableNum)
	for i := int32(0); i < testJobConfig.TableNum*testJobConfig.RecordCnt; i++ {
		data := executorCtx.RecvRecord(context.Background())
		r := data.(*runtime.Record)
		if i%10000 == 0 {
			log.L().Info("recv record", zap.Int32("table", r.Tid), zap.Int32("pk", r.Payload.(*pb.Record).Pk), zap.Int32("ith", i))
		}
		tablesCnt[r.Tid]++
	}
	for _, cnt := range tablesCnt {
		c.Assert(cnt, Equals, testJobConfig.RecordCnt)
	}

	// check job and task info written successfully.
	checkMetaStoreKeyNum(masterCtx.GetMetaKV(), adapter.JobKeyAdapter.Path(), 1, c)
	checkMetaStoreKeyNum(masterCtx.GetMetaKV(), adapter.TaskKeyAdapter.Path(), 1+int(testJobConfig.TableNum)+4*len(testJobConfig.Servers), c)

	resp1, err := client.CancelJob(context.Background(), &pb.CancelJobRequest{
		JobId: resp.JobId,
	})
	c.Assert(err, IsNil)
	c.Assert(resp1.Err, IsNil)
	cluster.StopCluster()
}

func getBenchmarkServers(n int, c *C) []string {
	ports, err := freeport.GetFreePorts(n)
	c.Assert(err, IsNil)
	servers := make([]string, 0, n)
	for i := 0; i < n; i++ {
		servers = append(servers, fmt.Sprintf("127.0.0.1:%d", ports[i]))
	}
	return servers
}

func (t *testJobSuite) TestPause(c *C) {
	cluster := NewEmptyMiniCluster()
	masterAddr, _, _, executorCtx := cluster.Start1M1E(c)
	client, err := client.NewMasterClient(context.Background(), []string{masterAddr})
	c.Assert(err, IsNil)
	testJobConfig := benchmark.Config{
		Servers:      getBenchmarkServers(3, c),
		FlowID:       "jobtest",
		TableNum:     10,
		RecordCnt:    100000,
		DDLFrequency: 100,
	}
	configBytes, err := json.Marshal(testJobConfig)
	c.Assert(err, IsNil)
	req := &pb.SubmitJobRequest{
		Tp:     pb.JobType_Benchmark,
		Config: configBytes,
	}
	resp, err := client.SubmitJob(context.Background(), req)
	c.Assert(err, IsNil)
	c.Assert(resp.Err, IsNil)
	susReq := &pb.PauseJobRequest{
		JobId: resp.JobId,
	}
	time.Sleep(100 * time.Millisecond)
	susResp, err := client.PauseJob(context.Background(), susReq)
	c.Assert(err, IsNil)
	c.Assert(susResp.Err, IsNil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	cnt := int32(0)
	for {
		data := executorCtx.RecvRecord(ctx)
		if data == nil {
			break
		}
		cnt++
	}
	c.Assert(cnt, Less, testJobConfig.TableNum*testJobConfig.RecordCnt)
	log.L().Logger.Info("has read", zap.Int32("cnt", cnt))
	time.Sleep(500 * time.Millisecond)
	c.Assert(executorCtx.TryRecvRecord(), IsNil)
	time.Sleep(500 * time.Millisecond)
	c.Assert(executorCtx.TryRecvRecord(), IsNil)
	cluster.StopCluster()
}

func checkMetaStoreKeyNum(store metadata.MetaKV, key string, valueNum int, c *C) {
	result, err := store.Get(context.Background(), key)
	c.Assert(err, IsNil)
	kvs := result.(*clientv3.GetResponse).Kvs
	c.Assert(len(kvs), Equals, valueNum)
}
