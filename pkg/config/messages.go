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

	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/p2p"
)

const defaultMaxRecvMsgSize = 256 * 1024 * 1024 // 256MB

// MessagesConfig configs MessageServer and MessageClient.
type MessagesConfig struct {
	ClientMaxBatchInterval TomlDuration `toml:"client-max-batch-interval" json:"client-max-batch-interval"`
	ClientMaxBatchSize     int          `toml:"client-max-batch-size" json:"client-max-batch-size"`
	ClientMaxBatchCount    int          `toml:"client-max-batch-count" json:"client-max-batch-count"`
	ClientRetryRateLimit   float64      `toml:"client-retry-rate-limit" json:"client-retry-rate-limit"`

	ServerMaxPendingMessageCount int          `toml:"server-max-pending-message-count" json:"server-max-pending-message-count"`
	ServerAckInterval            TomlDuration `toml:"server-ack-interval" json:"server-ack-interval"`
	ServerWorkerPoolSize         int          `toml:"server-worker-pool-size" json:"server-worker-pool-size"`

	// MaxRecvMsgSize is the maximum message size in bytes TiCDC can receive.
	MaxRecvMsgSize int `toml:"max-recv-msg-size" json:"max-recv-msg-size"`

	// After a duration of this time if the server doesn't see any activity it
	// pings the client to see if the transport is still alive.
	KeepAliveTime TomlDuration `toml:"keep-alive-time" json:"keep-alive-time"`
	// After having pinged for keepalive check, the server waits for a duration
	// of Timeout and if no activity is seen even after that the connection is
	// closed.
	KeepAliveTimeout TomlDuration `toml:"keep-alive-timeout" json:"keep-alive-timeout"`
}

// read only
var defaultMessageConfig = &MessagesConfig{
	// Note that ClientMaxBatchInterval may increase the checkpoint latency.
	ClientMaxBatchInterval:       TomlDuration(time.Millisecond * 10),
	ClientMaxBatchSize:           8 * 1024 * 1024, // 8MB
	ClientMaxBatchCount:          128,
	ClientRetryRateLimit:         1.0, // Once per second
	ServerMaxPendingMessageCount: 102400,
	ServerAckInterval:            TomlDuration(time.Millisecond * 100),
	ServerWorkerPoolSize:         4,
	MaxRecvMsgSize:               defaultMaxRecvMsgSize,
	KeepAliveTime:                TomlDuration(time.Second * 30),
	KeepAliveTimeout:             TomlDuration(time.Second * 10),
}

const (
	// These values are advanced parameters to MessageServer and MessageClient,
	// and it is not necessary for users to modify them.

	// clientSendChannelSize represents the size of an internal channel used to buffer
	// unsent messages.
	clientSendChannelSize = 128

	// clientDialTimeout represents the timeout given to gRPC to dial. 5 seconds seems reasonable
	// because it is unlikely that the latency between TiCDC nodes is larger than 5 seconds.
	clientDialTimeout = time.Second * 5

	// maxTopicPendingCount is the max allowed number of unhandled message for a message topic
	// ** if there is NO registered handler for it **.
	maxTopicPendingCount = 256

	// serverSendChannelSize is the size of a channel used to buffer messages to be sent back to
	// the client. Note that the traffic from the server to the client is minimal, as it consists
	// only of ACK messages.
	serverSendChannelSize = 16

	// maxPeerCount is the maximum number of peers that can be connected to the server.
	// 1024 is reasonable given the current scalability of TiCDC.
	maxPeerCount = 1024

	// unregisterHandleTimeout is the time to wait for a message handler to unregister.
	// Only in extreme situations can unregistering take more than a second. We use a timeout
	// to make deadlocking more detectable.
	unregisterHandleTimeout = time.Second * 10

	// serverSendRateLimit is the rate limit of sending messages from the server to the client.
	// Since ACK messages are batched, 1024 should be more than enough.
	serverSendRateLimit = 1024.0
)

// ValidateAndAdjust validates and adjusts the configs.
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
	if c.KeepAliveTime == 0 {
		c.KeepAliveTime = defaultMessageConfig.KeepAliveTime
	}
	if c.KeepAliveTimeout == 0 {
		c.KeepAliveTimeout = defaultMessageConfig.KeepAliveTimeout
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

	if c.MaxRecvMsgSize == 0 {
		c.MaxRecvMsgSize = defaultMaxRecvMsgSize
	}
	if c.MaxRecvMsgSize < 0 {
		return cerrors.ErrInvalidServerOption.GenWithStackByArgs(
			"max-recv-msg-size must be larger than 0")
	}

	return nil
}

// Clone returns a deep copy of the configuration.
func (c *MessagesConfig) Clone() *MessagesConfig {
	return &MessagesConfig{
		ClientMaxBatchInterval:       c.ClientMaxBatchInterval,
		ClientMaxBatchSize:           c.ClientMaxBatchSize,
		ClientMaxBatchCount:          c.ClientMaxBatchCount,
		ClientRetryRateLimit:         c.ClientRetryRateLimit,
		ServerMaxPendingMessageCount: c.ServerMaxPendingMessageCount,
		ServerAckInterval:            c.ServerAckInterval,
		ServerWorkerPoolSize:         c.ServerWorkerPoolSize,
		MaxRecvMsgSize:               c.MaxRecvMsgSize,
		KeepAliveTime:                c.KeepAliveTime,
		KeepAliveTimeout:             c.KeepAliveTimeout,
	}
}

// ToMessageClientConfig converts the MessagesConfig to a MessageClientConfig.
func (c *MessagesConfig) ToMessageClientConfig() *p2p.MessageClientConfig {
	return &p2p.MessageClientConfig{
		SendChannelSize:         clientSendChannelSize,
		BatchSendInterval:       time.Duration(c.ClientMaxBatchInterval),
		MaxBatchBytes:           c.ClientMaxBatchSize,
		MaxBatchCount:           c.ClientMaxBatchCount,
		RetryRateLimitPerSecond: c.ClientRetryRateLimit,
		DialTimeout:             clientDialTimeout,
		MaxRecvMsgSize:          c.MaxRecvMsgSize,
	}
}

// ToMessageServerConfig returns a MessageServerConfig that can be used to create a MessageServer.
func (c *MessagesConfig) ToMessageServerConfig() *p2p.MessageServerConfig {
	return &p2p.MessageServerConfig{
		MaxPendingMessageCountPerTopic:       maxTopicPendingCount,
		MaxPendingTaskCount:                  c.ServerMaxPendingMessageCount,
		SendChannelSize:                      serverSendChannelSize,
		AckInterval:                          time.Duration(c.ServerAckInterval),
		WorkerPoolSize:                       c.ServerWorkerPoolSize,
		MaxPeerCount:                         maxPeerCount,
		WaitUnregisterHandleTimeoutThreshold: unregisterHandleTimeout,
		SendRateLimitPerStream:               serverSendRateLimit,
		MaxRecvMsgSize:                       c.MaxRecvMsgSize,
		KeepAliveTimeout:                     time.Duration(c.KeepAliveTimeout),
		KeepAliveTime:                        time.Duration(c.KeepAliveTime),
	}
}
