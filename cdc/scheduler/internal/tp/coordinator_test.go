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
	send func(ctx context.Context, msgs []*schedulepb.Message) error
	recv func(ctx context.Context) ([]*schedulepb.Message, error)
}

func (m *mockTrans) Send(ctx context.Context, msgs []*schedulepb.Message) error {
	return m.send(ctx, msgs)
}
func (m *mockTrans) Recv(ctx context.Context) ([]*schedulepb.Message, error) {
	return m.recv(ctx)
}

func TestCoordinatorSendMsgs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	trans := &mockTrans{}
	cood := coordinator{
		version:  "6.2.0",
		revision: schedulepb.OwnerRevision{Revision: 3},
		trans:    trans,
	}
	trans.send = func(ctx context.Context, msgs []*schedulepb.Message) error {
		require.EqualValues(t, []*schedulepb.Message{{
			Header: &schedulepb.Message_Header{
				Version:       cood.version,
				OwnerRevision: cood.revision,
			},
			To: "1", MsgType: schedulepb.MsgDispatchTableRequest,
		}}, msgs)
		return nil
	}
	cood.sendMsgs(
		ctx, []*schedulepb.Message{{To: "1", MsgType: schedulepb.MsgDispatchTableRequest}})
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
	trans.recv = func(ctx context.Context) ([]*schedulepb.Message, error) {
		return []*schedulepb.Message{{
			Header: &schedulepb.Message_Header{
				OwnerRevision: cood.revision,
			},
			From: "1", MsgType: schedulepb.MsgDispatchTableResponse,
		}, {
			Header: &schedulepb.Message_Header{
				OwnerRevision: schedulepb.OwnerRevision{Revision: 4},
			},
			From: "2", MsgType: schedulepb.MsgDispatchTableResponse,
		}}, nil
	}
	msgs, err := cood.recvMsgs(ctx)
	require.Nil(t, err)
	require.EqualValues(t, []*schedulepb.Message{{
		Header: &schedulepb.Message_Header{
			OwnerRevision: cood.revision,
		},
		From: "1", MsgType: schedulepb.MsgDispatchTableResponse,
	}}, msgs)
}
