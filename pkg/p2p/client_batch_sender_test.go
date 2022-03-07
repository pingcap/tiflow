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
	"fmt"
	"math"
	"testing"

	proto "github.com/pingcap/tiflow/proto/p2p"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestClientBatchSenderMaxCount(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcStream := newMockSendMessageClient(ctx)
	sender := newClientBatchSender(grpcStream, 100, math.MaxInt64)

	grpcStream.AssertNotCalled(t, "Send", mock.Anything)
	// 99 messages
	for i := 1; i < 100; i++ {
		err := sender.Append(&proto.MessageEntry{
			Topic:    "test-topic",
			Content:  []byte(fmt.Sprintf("test-%d", i)),
			Sequence: int64(i),
		})
		require.NoError(t, err)
	}

	grpcStream.On("Send", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		msg := args.Get(0).(*proto.MessagePacket)
		require.Len(t, msg.Entries, 100)
		for i, entry := range msg.Entries {
			require.Equal(t, "test-topic", entry.Topic)
			require.Equal(t, int64(i+1), entry.Sequence)
			require.Equal(t, fmt.Sprintf("test-%d", i+1), string(entry.Content))
		}
	})
	// one more message
	err := sender.Append(&proto.MessageEntry{
		Topic:    "test-topic",
		Content:  []byte("test-100"),
		Sequence: 100,
	})
	require.NoError(t, err)
}

func TestClientBatchSenderMaxSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcStream := newMockSendMessageClient(ctx)
	sender := newClientBatchSender(grpcStream, math.MaxInt64, 1000)

	grpcStream.AssertNotCalled(t, "Send", mock.Anything)
	i := 1
	for size := 0; size < 220; {
		msg := &proto.MessageEntry{
			Topic:    "test-topic",
			Content:  []byte(fmt.Sprintf("test-%d", i)),
			Sequence: int64(i),
		}
		i++
		size += msg.Size()
		err := sender.Append(msg)
		require.NoError(t, err)
	}

	grpcStream.On("Send", mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		msg := args.Get(0).(*proto.MessagePacket)
		require.Len(t, msg.Entries, i)
		for i, entry := range msg.Entries {
			require.Equal(t, "test-topic", entry.Topic)
			require.Equal(t, int64(i+1), entry.Sequence)
			require.Equal(t, "11byteshere", string(entry.Content))
		}
	})
	// one more message
	err := sender.Append(&proto.MessageEntry{
		Topic:    "test-topic",
		Content:  []byte(fmt.Sprintf("test-%d", i)),
		Sequence: int64(i),
	})
	require.NoError(t, err)
}

func TestClientBatchSenderFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcStream := newMockSendMessageClient(ctx)
	sender := newClientBatchSender(grpcStream, math.MaxInt64, 1000)

	grpcStream.AssertNotCalled(t, "Send", mock.Anything)
	err := sender.Flush()
	require.NoError(t, err)
}
