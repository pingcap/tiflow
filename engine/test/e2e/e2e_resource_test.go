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
	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/test/resourcejob"
	"testing"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	pb "github.com/pingcap/tiflow/engine/enginepb"
)

func TestResourceJob(t *testing.T) {
	// TODO: make the following variables configurable
	var (
		masterAddrs              = []string{"127.0.0.1:10245", "127.0.0.1:10246", "127.0.0.1:10247"}
		userMetaAddrs            = []string{"127.0.0.1:12479"}
		userMetaAddrsInContainer = []string{"user-etcd-standalone:2379"}
	)

	ctx := context.Background()
	cfg := &resourcejob.JobConfig{
		ResourceCount: 10,
		ResourceLen:   10000,
	}
	cfgBytes, err := json.Marshal(cfg)
	require.NoError(t, err)

	masterClient, err := client.NewMasterClient(ctx, []string{"127.0.0.1:10245"})
	require.NoError(t, err)

	masterClient.SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:          pb.JobType(resourcejob.ResourceTestMasterType),
		Config:      nil,
		ProjectInfo: nil,
	})

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
