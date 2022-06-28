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
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/framework/fake"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/test/e2e"
)

func TestWorkerExit(t *testing.T) {
	// TODO: make the following variables configurable
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

		InjectErrorInterval: time.Second * 3,
	}
	cfgBytes, err := json.Marshal(cfg)
	require.NoError(t, err)

	fakeJobCfg := &e2e.FakeJobConfig{
		EtcdEndpoints: userMetaAddrs, // reuse user meta KV endpoints
		WorkerCount:   cfg.WorkerCount,
		KeyPrefix:     cfg.EtcdWatchPrefix,
	}
	cli, err := e2e.NewUTCli(ctx, masterAddrs, userMetaAddrs, tenant.DefaultUserProjectInfo, fakeJobCfg)
	require.NoError(t, err)

	jobID, err := cli.CreateJob(ctx, engineModel.JobTypeFakeJob, cfgBytes)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		// check tick increases to ensure all workers are online
		// TODO modify the test case to use a "restart-count" as a terminating condition.
		targetTick := int64(1000)
		for jobIdx := 0; jobIdx < cfg.WorkerCount; jobIdx++ {
			err := cli.CheckFakeJobTick(ctx, jobID, jobIdx, targetTick)
			if err != nil {
				log.L().Warn("check fake job tick failed", zap.Error(err))
				return false
			}
		}
		return true
	}, time.Second*300, time.Second*2)

	err = cli.PauseJob(ctx, jobID)
	require.NoError(t, err)
}
