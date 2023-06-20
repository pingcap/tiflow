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

package p2p

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
)

var _ MessageClient = &grpcMessageClient{}

// MessageClient is an interface for sending messages to a remote peer.
type MessageClient interface {
	// Run should be executed in a dedicated goroutine and it would block unless an irrecoverable error has been encountered.
	Run(ctx context.Context, network string, addr string, receiverID NodeID, credential *security.Credential) (ret error)

	// SendMessage sends a message of a given topic. It would block if the inner channel is congested.
	SendMessage(ctx context.Context, topic Topic, value interface{}) (seq Seq, ret error)

	// TrySendMessage tries to send a message of a given topic. It will return an error if the inner channel is congested.
	TrySendMessage(ctx context.Context, topic Topic, value interface{}) (seq Seq, ret error)

	// CurrentAck is used to query the latest sequence number for a topic that is acknowledged by the server.
	// Note: currently only used for test.
	CurrentAck(topic Topic) (Seq, bool)
}

// MessageClientConfig is used to configure MessageClient
type MessageClientConfig struct {
	// The size of the sending channel used to buffer
	// messages before they go to gRPC.
	SendChannelSize int
	// The maximum duration for which messages wait to be batched.
	BatchSendInterval time.Duration
	// The maximum size in bytes of a batch.
	MaxBatchBytes int
	// The maximum number of messages in a batch.
	MaxBatchCount int
	// The limit of the rate at which the connection to the server is retried.
	RetryRateLimitPerSecond float64
	// The dial timeout for the gRPC client
	DialTimeout time.Duration
	// The advertised address of this node. Used for logging and monitoring purposes.
	AdvertisedAddr string
	// The version of the client for compatibility check.
	// It should be in semver format. Empty string means no check.
	ClientVersion string
	// MaxRecvMsgSize is the maximum message size in bytes TiCDC can receive.
	MaxRecvMsgSize int
}

type localMessageClient struct {
	ctx     context.Context
	idx     atomic.Int64
	localCh chan RawMessageEntry
	// config is read only
	config *MessageClientConfig
}

func newLocalMessageClient(ctx context.Context, config *MessageClientConfig) MessageClient {
	return &localMessageClient{
		ctx:     ctx,
		idx:     atomic.Int64{},
		localCh: make(chan RawMessageEntry, config.SendChannelSize),
		config:  config,
	}
}

func (c *localMessageClient) Run(
	ctx context.Context, network string, addr string, receiverID NodeID, credential *security.Credential,
) error {
	return nil
}

func (c *localMessageClient) SendMessage(ctx context.Context, topic Topic, value interface{}) (Seq, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.ctx.Done():
		return 0, errors.WrapError(errors.ErrPeerMessageClientClosed, c.ctx.Err())
	case c.localCh <- RawMessageEntry{topic, value}:
		return c.idx.Add(1), nil
	}
}

func (c *localMessageClient) TrySendMessage(ctx context.Context, topic Topic, value interface{}) (Seq, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case c.localCh <- RawMessageEntry{topic, value}:
		return c.idx.Add(1), nil
	default:
		return 0, errors.ErrPeerMessageSendTryAgain.GenWithStackByArgs()
	}
}

func (c *localMessageClient) CurrentAck(topic Topic) (Seq, bool) {
	return c.idx.Load(), false
}
