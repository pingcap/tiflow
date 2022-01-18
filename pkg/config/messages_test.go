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

package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultMessageServerConfig(t *testing.T) {
	// This test case does a sanity check on default config.
	// In case there is any unsatisfied condition, the MessageServer will not work properly.
	serverConfig := defaultMessageConfig.ToMessageServerConfig()
	require.Greater(t, serverConfig.MaxPendingMessageCountPerTopic, 0)
	require.Greater(t, serverConfig.MaxPendingTaskCount, 0)
	require.Greater(t, serverConfig.SendChannelSize, 0)
	require.Greater(t, serverConfig.AckInterval, time.Duration(0))
	require.Less(t, serverConfig.AckInterval, 10*time.Second)
	require.Greater(t, serverConfig.WorkerPoolSize, 0)
	require.Greater(t, serverConfig.SendRateLimitPerStream, 0.1)
	require.Greater(t, serverConfig.MaxPeerCount, 0)
	require.Greater(t, serverConfig.WaitUnregisterHandleTimeoutThreshold, time.Duration(0))
}

func TestDefaultMessageClientConfig(t *testing.T) {
	clientConfig := defaultMessageConfig.ToMessageClientConfig()
	require.Greater(t, clientConfig.SendChannelSize, 0)
	require.Greater(t, clientConfig.BatchSendInterval, time.Duration(0))
	require.Greater(t, clientConfig.MaxBatchBytes, 0)
	require.Greater(t, clientConfig.MaxBatchCount, 0)
	require.Greater(t, clientConfig.RetryRateLimitPerSecond, 0.1)
	require.Greater(t, clientConfig.DialTimeout, time.Duration(0))
}

func TestMessagesConfigClone(t *testing.T) {
	config := defaultMessageConfig.Clone()
	require.Equal(t, defaultMessageConfig, config)
}

func TestMessagesConfigValidateAndAdjust(t *testing.T) {
	emptyConfig := &MessagesConfig{}
	err := emptyConfig.ValidateAndAdjust()
	require.NoError(t, err)
	require.Equal(t, defaultMessageConfig, emptyConfig)

	illegalConfig := defaultMessageConfig.Clone()
	illegalConfig.ServerAckInterval = TomlDuration(time.Second * 20)
	err = illegalConfig.ValidateAndAdjust()
	require.Error(t, err)
	require.Regexp(t, ".*ErrInvalidServerOption.*", err.Error())

	illegalConfig = defaultMessageConfig.Clone()
	illegalConfig.ClientMaxBatchInterval = TomlDuration(time.Second * 20)
	err = illegalConfig.ValidateAndAdjust()
	require.Error(t, err)
	require.Regexp(t, ".*ErrInvalidServerOption.*", err.Error())

	illegalConfig = defaultMessageConfig.Clone()
	illegalConfig.ServerWorkerPoolSize = 64
	err = illegalConfig.ValidateAndAdjust()
	require.Error(t, err)
	require.Regexp(t, ".*ErrInvalidServerOption.*", err.Error())
}
