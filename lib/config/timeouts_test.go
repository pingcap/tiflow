package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAdjustTimeoutConfig(t *testing.T) {
	t.Parallel()

	tc := TimeoutConfig{
		WorkerTimeoutDuration:            time.Second * 3,
		WorkerTimeoutGracefulDuration:    time.Second * 5,
		WorkerHeartbeatInterval:          time.Second * 3,
		WorkerReportStatusInterval:       time.Second * 3,
		MasterHeartbeatCheckLoopInterval: time.Second * 1,
	}
	expected := TimeoutConfig{
		WorkerTimeoutDuration:            time.Second * 9,
		WorkerTimeoutGracefulDuration:    time.Second * 5,
		WorkerHeartbeatInterval:          time.Second * 3,
		WorkerReportStatusInterval:       time.Second * 3,
		MasterHeartbeatCheckLoopInterval: time.Second * 1,
	}
	tc = tc.Adjust()
	require.Equal(t, expected, tc)
}
