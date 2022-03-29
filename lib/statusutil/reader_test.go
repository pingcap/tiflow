package statusutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	libModel "github.com/hanfei1991/microcosm/lib/model"
)

func TestReader(t *testing.T) {
	reader := NewReader(&libModel.WorkerStatus{
		Code:         libModel.WorkerStatusNormal,
		ErrorMessage: "test message",
	})
	require.Equal(t, &libModel.WorkerStatus{
		Code:         libModel.WorkerStatusNormal,
		ErrorMessage: "test message",
	}, reader.Status())

	err := reader.OnAsynchronousNotification(&libModel.WorkerStatus{
		Code: libModel.WorkerStatusFinished,
	})
	require.NoError(t, err)
	require.Equal(t, &libModel.WorkerStatus{
		Code:         libModel.WorkerStatusNormal,
		ErrorMessage: "test message",
	}, reader.Status())

	st, ok := reader.Receive()
	require.True(t, ok)
	require.Equal(t, &libModel.WorkerStatus{
		Code: libModel.WorkerStatusFinished,
	}, st)
}
