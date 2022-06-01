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
	"testing"

	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/stretchr/testify/require"
)

type mockTrans struct {
	sendBuffer []*schedulepb.Message
	recvBuffer []*schedulepb.Message
}

func newMockTrans() *mockTrans {
	return &mockTrans{
		sendBuffer: make([]*schedulepb.Message, 0),
		recvBuffer: make([]*schedulepb.Message, 0),
	}
}

func (m *mockTrans) Close() error {
	return nil
}

func (m *mockTrans) Send(ctx context.Context, msgs []*schedulepb.Message) error {
	m.sendBuffer = append(m.sendBuffer, msgs...)
	return nil
}

func (m *mockTrans) Recv(ctx context.Context) ([]*schedulepb.Message, error) {
	messages := m.recvBuffer[:len(m.recvBuffer)]
	m.recvBuffer = make([]*schedulepb.Message, 0)
	return messages, nil
}

func TestCoordinatorSendMsgs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	trans := newMockTrans()
	cood := coordinator{
		version:  "6.2.0",
		revision: schedulepb.OwnerRevision{Revision: 3},
		trans:    trans,
	}
	cood.sendMsgs(
		ctx, []*schedulepb.Message{{To: "1", MsgType: schedulepb.MsgDispatchTableRequest}})

	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			Version:       cood.version,
			OwnerRevision: cood.revision,
		},
		To: "1", MsgType: schedulepb.MsgDispatchTableRequest,
	}}, trans.sendBuffer)
}

func TestCoordinatorRecvMsgs(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	trans := &mockTrans{}
	cood := coordinator{
		version:  "6.2.0",
		revision: schedulepb.OwnerRevision{Revision: 3},
		trans:    trans,
	}

	trans.recvBuffer = append(trans.recvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: cood.revision,
			},
			From: "1", MsgType: schedulepb.MsgDispatchTableResponse,
		})
	trans.recvBuffer = append(trans.recvBuffer,
		&schedulepb.Message{
			Header: &schedulepb.Message_Header{
				OwnerRevision: schedulepb.OwnerRevision{Revision: 4},
			},
			From: "2", MsgType: schedulepb.MsgDispatchTableResponse,
		})

	msgs, err := cood.recvMsgs(ctx)
	require.NoError(t, err)

	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			OwnerRevision: cood.revision,
		},
		From: "1", MsgType: schedulepb.MsgDispatchTableResponse,
	}}, msgs)
}
