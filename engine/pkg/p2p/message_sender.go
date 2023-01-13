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

package p2p

import (
	"context"
	"time"

	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/security"
)

// MessageSender is used to send a message of a given topic to a given node.
type MessageSender interface {
	// TODO investigate whether we need to implement a barrier mechanism

	// SendToNode sends a message to a given node. Returns whether it is successful and a possible error.
	// A `would-block` error will not be returned. (false, nil) would be returned instead.
	SendToNode(ctx context.Context, targetNodeID NodeID, topic Topic, message interface{}) (bool, error)

	// SendToNodeB sends a message to a given node in a blocking way
	SendToNodeB(ctx context.Context, targetNodeID NodeID, topic Topic, message interface{}) error
}

type messageSenderImpl struct {
	router MessageRouter
}

// NewMessageSender returns a new message sender.
func NewMessageSender(router MessageRouter) MessageSender {
	return &messageSenderImpl{router: router}
}

// SendToNodeB implements MessageSender.SendToNodeB
// Note the blocking send may have performance issue, BE CAUTION when using this function.
func (m *messageSenderImpl) SendToNodeB(
	ctx context.Context, targetNodeID NodeID, topic Topic, message interface{},
) error {
	client := m.router.GetClient(targetNodeID)
	if client == nil {
		return errors.ErrExecutorNotFoundForMessage.GenWithStackByArgs()
	}

	// TODO: blocking send in p2p library may have performance issue
	_, err := client.SendMessage(ctx, topic, message)
	return err
}

func (m *messageSenderImpl) SendToNode(ctx context.Context, targetNodeID NodeID, topic Topic, message interface{}) (bool, error) {
	client := m.router.GetClient(targetNodeID)
	if client == nil {
		return false, nil
	}

	_, err := client.TrySendMessage(ctx, topic, message)
	if err != nil {
		if errors.Is(err, errors.ErrPeerMessageSendTryAgain) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// MessageRouter alias to p2p.MessageRouter
type MessageRouter = p2p.MessageRouter

var defaultClientConfig = &p2p.MessageClientConfig{
	SendChannelSize:         128,
	BatchSendInterval:       100 * time.Millisecond, // essentially disables flushing
	MaxBatchBytes:           8 * 1024 * 1024,        // 8MB
	MaxBatchCount:           4096,
	RetryRateLimitPerSecond: 1.0,             // once per second
	ClientVersion:           "v5.4.0",        // a fake version
	MaxRecvMsgSize:          4 * 1024 * 1024, // 4MB
}

// NewMessageRouter creates a new MessageRouter instance via tiflow p2p API
func NewMessageRouter(nodeID NodeID, advertisedAddr string) MessageRouter {
	config := *defaultClientConfig // copy
	config.AdvertisedAddr = advertisedAddr
	return p2p.NewMessageRouter(
		nodeID,
		&security.Credential{ /* TLS not supported for now */ },
		&config,
	)
}
