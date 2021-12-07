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
	"time"

	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/p2p"
)

// MessagesConfig configs MessageServer and MessageClient.
type MessagesConfig struct {
	ClientMaxBatchInterval TomlDuration `toml:"client-max-batch-interval" json:"client-max-batch-interval"`
	ClientMaxBatchSize     int          `toml:"client-max-batch-size" json:"client-max-batch-size"`
	ClientMaxBatchCount    int          `toml:"client-max-batch-count" json:"client-max-batch-count"`
	ClientRetryRateLimit   float64      `toml:"client-retry-rate-limit" json:"client-retry-rate-limit"`

	ServerMaxPendingMessageCount int          `toml:"server-max-pending-message-count" json:"server-max-pending-message-count"`
	ServerAckInterval            TomlDuration `toml:"server-ack-interval" json:"server-ack-interval"`
	ServerWorkerPoolSize         int          `toml:"server-worker-pool-size" json:"server-worker-pool-size"`
}

// read only
var defaultMessageConfig = &MessagesConfig{
	ClientMaxBatchInterval:       TomlDuration(time.Millisecond * 100),
	ClientMaxBatchSize:           8 * 1024, // 8MB
	ClientMaxBatchCount:          128,
	ClientRetryRateLimit:         1.0, // Once per second
	ServerMaxPendingMessageCount: 102400,
	ServerAckInterval:            TomlDuration(time.Millisecond * 100),
	ServerWorkerPoolSize:         4,
}

const (
	defaultClientSendChannelSize   = 128
	defaultClientDialTimeout       = time.Second * 3
	defaultMaxTopicPendingCount    = 256
	defaultServerSendChannelSize   = 16
	defaultMaxPeerCount            = 1024
	defaultUnregisterHandleTimeout = time.Second * 10
	defaultServerSendRateLimit     = 1024.0
)

func (c *MessagesConfig) ValidateAndAdjust() error {
	if c.ClientMaxBatchInterval == 0 {
		c.ClientMaxBatchInterval = defaultMessageConfig.ClientMaxBatchInterval
	}
	if time.Duration(c.ClientMaxBatchInterval) > 10*time.Second {
		return cerrors.ErrInvalidServerOption.GenWithStackByArgs("client-max-batch-interval is larger than 10s")
	}

	// We do not impose an upper limit on ClientMaxBatchSize and ClientMaxBatchCount
	// to allow some flexibility in tuning and debugging.
	if c.ClientMaxBatchSize <= 0 {
		c.ClientMaxBatchSize = defaultMessageConfig.ClientMaxBatchSize
	}

	if c.ClientMaxBatchCount <= 0 {
		c.ClientMaxBatchCount = defaultMessageConfig.ClientMaxBatchCount
	}

	if c.ClientRetryRateLimit <= 0.0 {
		c.ClientRetryRateLimit = defaultMessageConfig.ClientRetryRateLimit
	}

	if c.ServerMaxPendingMessageCount <= 0 {
		c.ServerMaxPendingMessageCount = defaultMessageConfig.ServerMaxPendingMessageCount
	}

	if c.ServerAckInterval == 0 {
		c.ServerAckInterval = defaultMessageConfig.ServerAckInterval
	}
	if time.Duration(c.ServerAckInterval) > 10*time.Second {
		return cerrors.ErrInvalidServerOption.GenWithStackByArgs("server-ack-interval is larger than 10s")
	}

	if c.ServerWorkerPoolSize <= 0 {
		c.ServerWorkerPoolSize = defaultMessageConfig.ServerWorkerPoolSize
	}
	// We put an upper limit on ServerWorkerPoolSize to avoid having to create many goroutines.
	if c.ServerWorkerPoolSize > 32 {
		return cerrors.ErrInvalidServerOption.GenWithStackByArgs("server-worker-pool-size is larger than 32")
	}

	return nil
}

func (c *MessagesConfig) Clone() *MessagesConfig {
	return &MessagesConfig{
		ClientMaxBatchInterval:       c.ClientMaxBatchInterval,
		ClientMaxBatchSize:           c.ClientMaxBatchSize,
		ClientMaxBatchCount:          c.ClientMaxBatchCount,
		ClientRetryRateLimit:         c.ClientRetryRateLimit,
		ServerMaxPendingMessageCount: c.ServerMaxPendingMessageCount,
		ServerAckInterval:            c.ServerAckInterval,
		ServerWorkerPoolSize:         c.ServerWorkerPoolSize,
	}
}

func (c *MessagesConfig) ToMessageClientConfig() *p2p.MessageClientConfig {
	return &p2p.MessageClientConfig{
		SendChannelSize:         defaultClientSendChannelSize,
		BatchSendInterval:       time.Duration(c.ClientMaxBatchInterval),
		MaxBatchBytes:           c.ClientMaxBatchSize,
		MaxBatchCount:           c.ClientMaxBatchCount,
		RetryRateLimitPerSecond: c.ClientRetryRateLimit,
		DialTimeout:             defaultClientDialTimeout,
	}
}

func (c *MessagesConfig) ToMessageServerConfig() *p2p.MessageServerConfig {
	return &p2p.MessageServerConfig{
		MaxPendingMessageCountPerTopic:       defaultMaxTopicPendingCount,
		MaxPendingTaskCount:                  c.ServerMaxPendingMessageCount,
		SendChannelSize:                      defaultServerSendChannelSize,
		AckInterval:                          time.Duration(c.ServerAckInterval),
		WorkerPoolSize:                       c.ServerWorkerPoolSize,
		MaxPeerCount:                         defaultMaxPeerCount,
		WaitUnregisterHandleTimeoutThreshold: defaultUnregisterHandleTimeout,
		SendRateLimitPerStream:               defaultServerSendRateLimit,
	}
}
