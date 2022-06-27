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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/p2p"
	"go.uber.org/zap"
)

type transport interface {
	Send(ctx context.Context, msgs []*schedulepb.Message) error
	Recv(ctx context.Context) ([]*schedulepb.Message, error)
	Close() error
}

func p2pTopic(changefeed model.ChangeFeedID, role Role) (selfTopic, peerTopic p2p.Topic) {
	if role == agentRole {
		selfTopic = fmt.Sprintf(
			"changefeed/%s/%s/%s", changefeed.Namespace, changefeed.ID, agentRole)
		peerTopic = fmt.Sprintf(
			"changefeed/%s/%s/%s", changefeed.Namespace, changefeed.ID, schedulerRole)
	} else {
		selfTopic = fmt.Sprintf(
			"changefeed/%s/%s/%s", changefeed.Namespace, changefeed.ID, schedulerRole)
		peerTopic = fmt.Sprintf(
			"changefeed/%s/%s/%s", changefeed.Namespace, changefeed.ID, agentRole)
	}
	return
}

var _ transport = (*p2pTransport)(nil)

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
	agentRole     Role = "agent"
	schedulerRole Role = "scheduler"
)

func newTransport(
	ctx context.Context, changefeed model.ChangeFeedID, role Role,
	server *p2p.MessageServer, router p2p.MessageRouter,
) (*p2pTransport, error) {
	selfTopic, peerTopic := p2pTopic(changefeed, role)
	trans := &p2pTransport{
		changefeed:    changefeed,
		selfTopic:     selfTopic,
		peerTopic:     peerTopic,
		messageServer: server,
		messageRouter: router,
	}
	var err error
	trans.errCh, err = trans.messageServer.SyncAddHandler(
		ctx,
		trans.selfTopic,
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
	ctx context.Context, msgs []*schedulepb.Message,
) error {
	for i := range msgs {
		value := msgs[i]
		to := value.To
		client := t.messageRouter.GetClient(to)
		if client == nil {
			log.Warn("tpscheduler: no message client found, retry later",
				zap.String("namespace", t.changefeed.Namespace),
				zap.String("changefeed", t.changefeed.ID),
				zap.String("to", to))
			continue
		}

		_, err := client.TrySendMessage(ctx, t.peerTopic, value)
		if err != nil {
			if cerror.ErrPeerMessageSendTryAgain.Equal(err) {
				return nil
			}
			if cerror.ErrPeerMessageClientClosed.Equal(err) {
				log.Warn("tpscheduler: peer messaging client is closed"+
					"while trying to send a message through it. "+
					"Report a bug if this warning repeats",
					zap.String("namespace", t.changefeed.Namespace),
					zap.String("changefeed", t.changefeed.ID),
					zap.String("to", to))
				return nil
			}
			return errors.Trace(err)
		}
	}

	if len(msgs) != 0 {
		log.Debug("tpscheduler: all messages sent",
			zap.String("namespace", t.changefeed.Namespace),
			zap.String("changefeed", t.changefeed.ID),
			zap.Int("len", len(msgs)))
	}
	return nil
}

func (t *p2pTransport) Recv(ctx context.Context) ([]*schedulepb.Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	recvMsgs := t.mu.msgBuf
	t.mu.msgBuf = make([]*schedulepb.Message, 0)
	return recvMsgs, nil
}

func (t *p2pTransport) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := t.messageServer.SyncRemoveHandler(ctx, t.selfTopic)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
