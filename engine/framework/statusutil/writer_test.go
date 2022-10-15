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

package statusutil

import (
	"context"
	"testing"

	"github.com/pingcap/tiflow/engine/framework/internal/worker"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

type writerTestSuite struct {
	writer        *Writer
	cli           pkgOrm.Client
	messageSender *p2p.MockMessageSender
	masterInfo    *worker.MockMasterInfoProvider
}

func newWriterTestSuite(
	t *testing.T,
	masterID frameModel.MasterID,
	masterNode p2p.NodeID,
	masterEpoch frameModel.Epoch,
	workerID frameModel.WorkerID,
) *writerTestSuite {
	cli, err := pkgOrm.NewMockClient()
	require.NoError(t, err)

	messageSender := p2p.NewMockMessageSender()
	masterInfo := worker.NewMockMasterInfoProvider(masterID, masterNode, masterEpoch)
	return &writerTestSuite{
		writer:        NewWriter(cli, messageSender, masterInfo, workerID),
		cli:           cli,
		messageSender: messageSender,
		masterInfo:    masterInfo,
	}
}

func (s *writerTestSuite) Close() {
	_ = s.cli.Close()
}

func TestWriterUpdate(t *testing.T) {
	suite := newWriterTestSuite(t, "master-1", "executor-1", 1, "worker-1")
	defer suite.Close()
	ctx := context.Background()

	st := &frameModel.WorkerStatus{
		JobID:    "master-1",
		ID:       "worker-1",
		State:    frameModel.WorkerStateNormal,
		ErrorMsg: "test",
	}

	err := suite.cli.UpsertWorker(ctx, st)
	require.NoError(t, err)

	st, err = suite.cli.GetWorkerByID(ctx, st.JobID, st.ID)
	require.NoError(t, err)

	err = suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)

	status, err := suite.cli.GetWorkerByID(ctx, st.JobID, st.ID)
	require.NoError(t, err)
	require.Equal(t, status.State, frameModel.WorkerStateNormal)
	require.Equal(t, status.ErrorMsg, "test")

	rawMsg, ok := suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg := rawMsg.(*WorkerStatusMessage)
	checkWorkerStatusMsg(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)

	// Deletes the persisted status for testing purpose.
	// TODO make a better mock KV that can inspect calls.
	_, err = suite.cli.DeleteWorker(ctx, st.JobID, st.ID)
	require.NoError(t, err)

	// Repeated update. Should have a notification too, but no persistence.
	err = suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)
	_, ok = suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg = rawMsg.(*WorkerStatusMessage)
	checkWorkerStatusMsg(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)
	_, err = suite.cli.GetWorkerByID(ctx, st.JobID, st.ID)
	require.Error(t, err)
}

func TestWriterSendRetry(t *testing.T) {
	suite := newWriterTestSuite(t, "master-1", "executor-1", 1, "worker-1")
	defer suite.Close()
	ctx := context.Background()

	st := &frameModel.WorkerStatus{
		JobID:    "master-1",
		ID:       "worker-1",
		State:    frameModel.WorkerStateNormal,
		ErrorMsg: "test",
	}
	err := suite.cli.UpsertWorker(ctx, st)
	require.NoError(t, err)

	st, err = suite.cli.GetWorkerByID(ctx, st.JobID, st.ID)
	require.NoError(t, err)

	require.Equal(t, 0, suite.masterInfo.RefreshCount())
	suite.messageSender.InjectError(errors.ErrExecutorNotFoundForMessage.GenWithStackByArgs())
	err = suite.writer.UpdateStatus(ctx, st)
	require.NoError(t, err)
	require.Equal(t, 1, suite.masterInfo.RefreshCount())

	rawMsg, ok := suite.messageSender.TryPop("executor-1", WorkerStatusTopic("master-1"))
	require.True(t, ok)
	msg := rawMsg.(*WorkerStatusMessage)
	checkWorkerStatusMsg(t, &WorkerStatusMessage{
		Worker:      "worker-1",
		MasterEpoch: 1,
		Status:      st,
	}, msg)
}

func checkWorkerStatusMsg(t *testing.T, expect, msg *WorkerStatusMessage) {
	require.Equal(t, expect.Worker, msg.Worker)
	require.Equal(t, expect.MasterEpoch, msg.MasterEpoch)
	require.Equal(t, expect.Status.State, expect.Status.State)
	require.Equal(t, expect.Status.ErrorMsg, expect.Status.ErrorMsg)
	require.Equal(t, expect.Status.ExtBytes, expect.Status.ExtBytes)
}
