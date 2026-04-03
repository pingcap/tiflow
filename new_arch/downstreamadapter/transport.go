// Copyright 2024 PingCAP, Inc.
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

package downstreamadapter

import (
	"context"
	"sync"

	"github.com/pingcap/tiflow/new_arch/downstreamadapter/messages"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/p2p"
)

// Transport is an interface of message channel between scheduler and agent.
type Transport interface {
	Send(ctx context.Context, msgs []*messages.HeartBeatMessage) error
	Recv(ctx context.Context) ([]*messages.HeartBeatResponse, error)
	Close() error
}

// TODO 逻辑读一下然后改写
func p2pTopic(changefeed model.ChangeFeedID, role Role) (selfTopic, peerTopic p2p.Topic) {
	return
}

var _ Transport = (*p2pTransport)(nil)

type p2pTransport struct {
	changefeed    model.ChangeFeedID
	selfTopic     p2p.Topic
	peerTopic     p2p.Topic
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter
	errCh         <-chan error

	mu struct {
		sync.Mutex
		// FIXME it's an unbounded buffer, and may cause OOM!
		msgBuf []*schedulepb.Message
	}
}

// Role of the transport user.
type Role string

const (
	// AgentRole is the role of agent.
	AgentRole Role = "agent"
	// SchedulerRole is the role of scheduler.
	SchedulerRole Role = "scheduler"
)

// NewTransport returns a new transport.
func NewTransport(
	ctx context.Context, changefeed model.ChangeFeedID, role Role,
	server *p2p.MessageServer, router p2p.MessageRouter,
) (*p2pTransport, error) {
}

func (t *p2pTransport) Send(
	ctx context.Context, msgs []*schedulepb.Message,
) error {
}

func (t *p2pTransport) Recv(ctx context.Context) ([]*schedulepb.Message, error) {
}

func (t *p2pTransport) Close() error {
}
