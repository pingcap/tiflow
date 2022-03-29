package lib

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdjustTimeoutConfig(t *testing.T) {
	t.Parallel()

	tc := TimeoutConfig{
		workerTimeoutDuration:            time.Second * 3,
		workerTimeoutGracefulDuration:    time.Second * 5,
		workerHeartbeatInterval:          time.Second * 3,
		workerReportStatusInterval:       time.Second * 3,
		masterHeartbeatCheckLoopInterval: time.Second * 1,
	}
	expected := TimeoutConfig{
		workerTimeoutDuration:            time.Second * 9,
		workerTimeoutGracefulDuration:    time.Second * 5,
		workerHeartbeatInterval:          time.Second * 3,
		workerReportStatusInterval:       time.Second * 3,
		masterHeartbeatCheckLoopInterval: time.Second * 1,
	}
	tc = tc.Adjust()
	require.Equal(t, expected, tc)
}
