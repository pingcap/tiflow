package dm

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/jobmaster/dm/runtime"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
)

func TestMessageAgent(t *testing.T) {
	messageAgent := NewMessageAgent(&MockSender{})

	// Test QueryStatus
	dumpStatus := &runtime.DumpStatus{
		DefaultTaskStatus: runtime.DefaultTaskStatus{
			Unit:  lib.WorkerDMDump,
			Task:  "task1",
			Stage: metadata.StageRunning,
		},
	}
	require.Nil(t, messageAgent.OperateTaskResponse(context.Background(), 1, dumpStatus, ""))
	require.Nil(t, messageAgent.OperateTaskResponse(context.Background(), 1, nil, "status error"))
}

type MockSender struct {
	id libModel.WorkerID
}

func (s *MockSender) ID() libModel.WorkerID {
	return s.id
}

func (s *MockSender) SendMessage(ctx context.Context, topic string, message interface{}, nonblocking bool) error {
	return nil
}
