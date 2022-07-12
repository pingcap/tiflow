// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework/fake"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/test/e2e"
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
				log.Warn("check fake job failed", zap.Error(err))
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
		masterAddrs                  = []string{"127.0.0.1:10245", "127.0.0.1:10246", "127.0.0.1:10247"}
		businessMetaAddrs            = []string{"127.0.0.1:12479"}
		businessMetaAddrsInContainer = []string{"business-etcd-standalone:2379"}
	)

	seed := time.Now().Unix()
	rand.Seed(seed)
	log.Info("set random seed", zap.Int64("seed", seed))

	ctx := context.Background()
	cfg := &fake.Config{
		JobName:     "test-node-failure",
		WorkerCount: 4,
		// use a large enough target tick to ensure the fake job long running
		TargetTick:      10000000,
		EtcdWatchEnable: true,
		EtcdEndpoints:   businessMetaAddrsInContainer,
		EtcdWatchPrefix: "/fake-job/test/",
	}
	cfgBytes, err := json.Marshal(cfg)
	require.NoError(t, err)

	fakeJobCfg := &e2e.FakeJobConfig{
		EtcdEndpoints: businessMetaAddrs, // reuse business meta KV endpoints
		WorkerCount:   cfg.WorkerCount,
		KeyPrefix:     cfg.EtcdWatchPrefix,
	}

	cli, err := e2e.NewUTCli(ctx, masterAddrs, businessMetaAddrs, tenant.DefaultUserProjectInfo,
		fakeJobCfg)
	require.NoError(t, err)

	jobID, err := cli.CreateJob(ctx, engineModel.JobTypeFakeJob, cfgBytes)
	require.NoError(t, err)

	err = cli.InitializeMetaClient(jobID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		// check tick increases to ensure all workers are online
		targetTick := int64(20)
		for jobIdx := 0; jobIdx < cfg.WorkerCount; jobIdx++ {
			err := cli.CheckFakeJobTick(ctx, jobID, jobIdx, targetTick)
			if err != nil {
				log.Warn("check fake job tick failed", zap.Error(err))
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
		cli.ContainerRestart(masterContainerName(i))
		mvccCount++
		value := fmt.Sprintf("restart-server-master-value-%d", i)
		updateKeyAndCheckOnce(ctx, t, cli, jobID, cfg.WorkerCount, value, mvccCount)
	}

	// transfer etcd leader to a random node for several times
	for i := 0; i < nodeCount; i++ {
		err := cli.TransferEtcdLeader(ctx)
		require.NoError(t, err)
		mvccCount++
		value := fmt.Sprintf("transfer-etcd-leader-value-%d", i)
		updateKeyAndCheckOnce(ctx, t, cli, jobID, cfg.WorkerCount, value, mvccCount)
	}

	// restart all executors and check fake job is running normally
	for i := 0; i < nodeCount; i++ {
		cli.ContainerRestart(executorContainerName(i))
		mvccCount++
		value := fmt.Sprintf("restart-executor-value-%d", i)
		updateKeyAndCheckOnce(ctx, t, cli, jobID, cfg.WorkerCount, value, mvccCount)
	}

	for i := 0; i < nodeCount; i++ {
		cli.ContainerStop(masterContainerName(i))
		cli.ContainerStop(executorContainerName(i))
	}
	for i := 0; i < nodeCount; i++ {
		cli.ContainerStart(masterContainerName(i))
		cli.ContainerStart(executorContainerName(i))
	}
	value := "stop-start-container-value"
	mvccCount++
	updateKeyAndCheckOnce(ctx, t, cli, jobID, cfg.WorkerCount, value, mvccCount)

	err = cli.PauseJob(ctx, jobID)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		stopped, err := cli.CheckJobStatus(ctx, jobID, pb.QueryJobResponse_stopped)
		if err != nil {
			log.Warn("check job status failed", zap.Error(err))
			return false
		}
		if !stopped {
			log.Info("job is not stopped")
			return false
		}
		return true
	}, time.Second*60, time.Second*2)
}

func masterContainerName(index int) string {
	return fmt.Sprintf("sample_server-master-%d_1", index)
}

func executorContainerName(index int) string {
	return fmt.Sprintf("sample_server-executor-%d_1", index)
}
