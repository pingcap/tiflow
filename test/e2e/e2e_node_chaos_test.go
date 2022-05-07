package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/hanfei1991/microcosm/lib/fake"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/test/e2e"
)

// update the watched key of workers belonging to a given job, and then
// check the mvcc count and value of the key are updated as expected.
func updateKeyAndCheckOnce(
	ctx context.Context, t *testing.T, cli *e2e.ChaosCli,
	jobID string, workerCount int, updateValue string, expectedMvcc int,
) {
	for j := 0; j < workerCount; j++ {
		err := cli.UpdateFakeJobKey(ctx, j, updateValue)
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		for jobIdx := 0; jobIdx < workerCount; jobIdx++ {
			err := cli.CheckFakeJobKey(ctx, jobID, jobIdx, expectedMvcc, updateValue)
			if err != nil {
				log.L().Warn("check fake job failed", zap.Error(err))
				return false
			}
		}
		return true
	}, time.Second*60, time.Second*2)
}

func TestNodeFailure(t *testing.T) {
	// TODO: make the following variables configurable, these variables keep the
	// same in sample/3m3e.yaml
	var (
		masterAddrs              = []string{"127.0.0.1:10245", "127.0.0.1:10246", "127.0.0.1:10247"}
		userMetaAddrs            = []string{"127.0.0.1:12479"}
		userMetaAddrsInContainer = []string{"user-etcd-standalone:2379"}
	)

	ctx := context.Background()
	cfg := &fake.Config{
		JobName:     "test-node-failure",
		WorkerCount: 4,
		// use a large enough target tick to ensure the fake job long running
		TargetTick:      10000000,
		EtcdWatchEnable: true,
		EtcdEndpoints:   userMetaAddrsInContainer,
		EtcdWatchPrefix: "/fake-job/test/",
	}
	cfgBytes, err := json.Marshal(cfg)
	require.NoError(t, err)

	fakeJobCfg := &e2e.FakeJobConfig{
		EtcdEndpoints: userMetaAddrs, // reuse user meta KV endpoints
		WorkerCount:   cfg.WorkerCount,
		KeyPrefix:     cfg.EtcdWatchPrefix,
	}
	cli, err := e2e.NewUTCli(ctx, masterAddrs, userMetaAddrs, fakeJobCfg)
	require.NoError(t, err)

	jobID, err := cli.CreateJob(ctx, pb.JobType_FakeJob, cfgBytes)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		// check tick increases to ensure all workers are online
		targetTick := int64(20)
		for jobIdx := 0; jobIdx < cfg.WorkerCount; jobIdx++ {
			err := cli.CheckFakeJobTick(ctx, jobID, jobIdx, targetTick)
			if err != nil {
				log.L().Warn("check fake job tick failed", zap.Error(err))
				return false
			}
		}
		return true
	}, time.Second*60, time.Second*2)

	mvccCount := 1
	updateKeyAndCheckOnce(ctx, t, cli, jobID, cfg.WorkerCount, "random-value-1", mvccCount)

	// restart all server masters and check fake job is running normally
	nodeCount := 3
	for i := 0; i < nodeCount; i++ {
		name := fmt.Sprintf("sample_server-master-%d_1", i)
		cli.ContainerRestart(name)
		mvccCount++
		value := fmt.Sprintf("restart-server-master-value-%d", i)
		updateKeyAndCheckOnce(ctx, t, cli, jobID, cfg.WorkerCount, value, mvccCount)
	}

	// restart all executors and check fake job is running normally
	for i := 0; i < nodeCount; i++ {
		name := fmt.Sprintf("sample_server-executor-%d_1", i)
		cli.ContainerRestart(name)
		mvccCount++
		value := fmt.Sprintf("restart-executor-value-%d", i)
		updateKeyAndCheckOnce(ctx, t, cli, jobID, cfg.WorkerCount, value, mvccCount)
	}

	err = cli.PauseJob(ctx, jobID)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		stopped, err := cli.CheckJobStatus(ctx, jobID, pb.QueryJobResponse_stopped)
		if err != nil {
			log.L().Warn("check job status failed", zap.Error(err))
			return false
		}
		if !stopped {
			log.L().Info("job is not stopped")
			return false
		}
		return true
	}, time.Second*60, time.Second*2)
}
