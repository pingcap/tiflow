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

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/client"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/test/resourcejob"
)

func TestResourceJob(t *testing.T) {
	ctx := context.Background()
	cfg := &resourcejob.JobConfig{
		ResourceCount: 10,
		ResourceLen:   10000,
	}
	cfgBytes, err := json.Marshal(cfg)
	require.NoError(t, err)

	masterClient, err := client.NewMasterClient(ctx, []string{"127.0.0.1:10245"})
	require.NoError(t, err)

	resp, err := masterClient.SubmitJob(ctx, &pb.SubmitJobRequest{
		Tp:     pb.JobType_ResourceTestJob,
		Config: cfgBytes,
	})
	require.NoError(t, err)
	require.Nilf(t, resp.Err, "%s", resp.Err.String())

	jobID := resp.JobId

	require.Eventually(t, func() bool {
		resp, err := masterClient.QueryJob(ctx, &pb.QueryJobRequest{
			JobId: jobID,
		})
		if err != nil {
			log.L().Info("QueryJob met error", log.ShortError(err))
			return false
		}
		if resp.Err != nil {
			log.L().Info("QueryJob responded with error", zap.Any("err", resp.Err))
			return false
		}

		if resp.Status == pb.QueryJobResponse_finished {
			return true
		}
		log.L().Info("QueryJob",
			zap.String("job-id", jobID),
			zap.Any("resp", resp))
		return false
	}, time.Second*300, time.Second*2)

	_, err = masterClient.PauseJob(ctx, &pb.PauseJobRequest{JobId: jobID})
	require.NoError(t, err)
}
