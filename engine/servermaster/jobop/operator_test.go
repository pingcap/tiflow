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

package jobop

import (
	"context"
	"testing"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

type mockOperatorRouter struct {
	onlineJobs  map[string]struct{}
	cancelCalls map[string]int
	cli         pkgOrm.Client
}

func newMockOperatorRouter(cli pkgOrm.Client) *mockOperatorRouter {
	return &mockOperatorRouter{
		onlineJobs:  make(map[string]struct{}),
		cancelCalls: make(map[string]int),
		cli:         cli,
	}
}

func (r *mockOperatorRouter) SendCancelJobMessage(
	ctx context.Context, jobID string,
) error {
	if _, ok := r.onlineJobs[jobID]; !ok {
		return errors.ErrMasterNotFound.GenWithStackByArgs(jobID)
	}
	r.cancelCalls[jobID]++
	return nil
}

func (r *mockOperatorRouter) checkCancelCalls(t *testing.T, jobID string, expected int) {
	require.Contains(t, r.cancelCalls, jobID)
	require.Equal(t, expected, r.cancelCalls[jobID])
}

func (r *mockOperatorRouter) jobOnline(
	ctx context.Context, jobID string, meta *frameModel.MasterMeta,
) error {
	r.onlineJobs[jobID] = struct{}{}
	return r.cli.UpsertJob(ctx, meta)
}

func (r *mockOperatorRouter) jobOffline(
	ctx context.Context, jobID string, meta *frameModel.MasterMeta,
) error {
	delete(r.onlineJobs, jobID)
	return r.cli.UpsertJob(ctx, meta)
}

func checkJobOpWithStatus(
	ctx context.Context, t *testing.T, metaCli pkgOrm.Client,
	expectedStatus ormModel.JobOpStatus, expectedCount int,
) {
	ops, err := metaCli.QueryJobOpsByStatus(ctx, expectedStatus)
	require.NoError(t, err)
	require.Len(t, ops, expectedCount)
}

func TestJobOperator(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	metaCli, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	router := newMockOperatorRouter(metaCli)
	oper := NewJobOperatorImpl(metaCli, router)

	jobID := "cancel-job-id"
	meta := &frameModel.MasterMeta{
		ID:    jobID,
		Type:  frameModel.CvsJobMaster,
		State: frameModel.MasterStateInit,
	}
	err = router.jobOnline(ctx, jobID, meta)
	require.NoError(t, err)

	require.False(t, oper.IsJobCanceling(ctx, jobID))
	err = oper.MarkJobCanceling(ctx, jobID)
	require.NoError(t, err)
	// cancel job repeatly is ok
	err = oper.MarkJobCanceling(ctx, jobID)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		err = oper.Tick(ctx)
		require.NoError(t, err)
		router.checkCancelCalls(t, jobID, i+1)
		checkJobOpWithStatus(ctx, t, metaCli, ormModel.JobOpStatusCanceling, 1)
		require.True(t, oper.IsJobCanceling(ctx, jobID))
	}

	// mock job master is canceled and status persisted
	meta.State = frameModel.MasterStateStopped
	err = router.jobOffline(ctx, jobID, meta)
	require.NoError(t, err)

	err = oper.Tick(ctx)
	require.NoError(t, err)
	checkJobOpWithStatus(ctx, t, metaCli, ormModel.JobOpStatusCanceled, 1)
	require.False(t, oper.IsJobCanceling(ctx, jobID))
}

func TestJobOperatorMetOrphanJob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	metaCli, err := pkgOrm.NewMockClient()
	require.NoError(t, err)
	router := newMockOperatorRouter(metaCli)
	oper := NewJobOperatorImpl(metaCli, router)

	jobID := "cancel-orphan-job-id"
	err = oper.MarkJobCanceling(ctx, jobID)
	require.NoError(t, err)

	err = oper.Tick(ctx)
	require.NoError(t, err)
	checkJobOpWithStatus(ctx, t, metaCli, ormModel.JobOpStatusNoop, 1)
}
