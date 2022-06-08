// Copyright 2021 PingCAP, Inc.
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

package binlogstream

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/relay"
	"github.com/stretchr/testify/require"
)

func TestToBinlogType(t *testing.T) {
	testCases := []struct {
		relay relay.Process
		tp    BinlogType
	}{
		{
			&relay.Relay{},
			LocalBinlog,
		}, {
			nil,
			RemoteBinlog,
		},
	}

	for _, testCase := range testCases {
		tp := RelayToBinlogType(testCase.relay)
		require.Equal(t, testCase.tp, tp)
	}
}

func TestCanErrorRetry(t *testing.T) {
	relay2 := &relay.Relay{}
	controller := NewStreamerController(replication.BinlogSyncerConfig{}, true, nil, "", nil, relay2)

	mockErr := errors.New("test")

	// local binlog puller can always retry
	for i := 0; i < 5; i++ {
		require.True(t, controller.CanRetry(mockErr))
	}

	origCfg := minErrorRetryInterval
	minErrorRetryInterval = 100 * time.Millisecond
	defer func() {
		minErrorRetryInterval = origCfg
	}()

	// test with remote binlog
	controller = NewStreamerController(replication.BinlogSyncerConfig{}, true, nil, "", nil, nil)

	require.True(t, controller.CanRetry(mockErr))
	require.False(t, controller.CanRetry(mockErr))
	time.Sleep(100 * time.Millisecond)
	require.True(t, controller.CanRetry(mockErr))
}
