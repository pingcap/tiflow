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

package tp

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/p2p"
)

type transport interface {
	Send(ctx context.Context, to model.CaptureID, msgs []*schedulepb.Message) error
	Recv(ctx context.Context, msgs []*schedulepb.Message) error
}

func p2pTopic(changefeed model.ChangeFeedID) p2p.Topic {
	return fmt.Sprintf("changefeed/%s/%s", changefeed.Namespace, changefeed.ID)
}

type p2pTransport struct {
	topic         p2p.Topic
	messageServer *p2p.MessageServer
	messageRouter p2p.MessageRouter
	errCh         <-chan error

	mu struct {
		sync.Mutex
		// FIXME it's an unbounded buffer, and may cuase OOM!
		msgBuf []*schedulepb.Message
	}
}

func newTranport(
	ctx context.Context, changefeed model.ChangeFeedID,
	server *p2p.MessageServer, router p2p.MessageRouter,
) (transport, error) {
	trans := &p2pTransport{
		topic:         p2pTopic(changefeed),
		messageServer: server,
		messageRouter: router,
	}
	var err error
	trans.errCh, err = trans.messageServer.SyncAddHandler(
		ctx,
		trans.topic,
		&schedulepb.Message{},
		func(sender string, messageI interface{}) error {
			message := messageI.(*schedulepb.Message)
			trans.mu.Lock()
			trans.mu.msgBuf = append(trans.mu.msgBuf, message)
			trans.mu.Unlock()
			return nil
		})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return trans, nil
}

func (t *p2pTransport) Send(
	ctx context.Context, to model.CaptureID, msgs []*schedulepb.Message,
) error {
	return nil
}

func (t *p2pTransport) Recv(
	ctx context.Context, msgs []*schedulepb.Message,
) error {
	return nil
}
