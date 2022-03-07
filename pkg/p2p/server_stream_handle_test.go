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
	"sync"
	"testing"
	"time"

	proto "github.com/pingcap/tiflow/proto/p2p"
	"github.com/stretchr/testify/require"
)

var mockStreamMeta = &proto.StreamMeta{
	SenderId:   "node-1",
	ReceiverId: "node-2",
	Epoch:      1,
}

func TestStreamHandleSend(t *testing.T) {
	t.Parallel()
	// We use a blocking channel to make the test case deterministic.
	sendCh := make(chan proto.SendMessageResponse)
	h := newStreamHandle(mockStreamMeta, sendCh)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		msg := <-sendCh
		require.Equal(t, "test", msg.ErrorMessage)
	}()

	err := h.Send(ctx, proto.SendMessageResponse{
		ErrorMessage: "test",
	})
	require.NoError(t, err)

	wg.Wait()
	cancel()
	err = h.Send(ctx, proto.SendMessageResponse{})
	require.Error(t, err, "should have been cancelled")
	require.Regexp(t, "context canceled", err.Error())
}

func TestStreamHandleCloseWhileSending(t *testing.T) {
	t.Parallel()
	// We use a blocking channel to make the test case deterministic.
	sendCh := make(chan proto.SendMessageResponse)
	h := newStreamHandle(mockStreamMeta, sendCh)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := h.Send(ctx, proto.SendMessageResponse{})
		require.Error(t, err)
		require.Regexp(t, "ErrPeerMessageInternalSenderClosed", err.Error())
	}()

	time.Sleep(100 * time.Millisecond)
	h.Close()

	wg.Wait()
	// Tests double close.
	// Should not panic.
	h.Close()
}

func TestStreamHandleCloseWhileNotSending(t *testing.T) {
	t.Parallel()
	// We use a blocking channel to make the test case deterministic.
	sendCh := make(chan proto.SendMessageResponse)
	h := newStreamHandle(mockStreamMeta, sendCh)

	h.Close()

	// Tests double close.
	// Should not panic.
	h.Close()
}

func TestGetStreamMeta(t *testing.T) {
	t.Parallel()
	h := newStreamHandle(mockStreamMeta, nil)
	require.Equal(t, mockStreamMeta, h.GetStreamMeta())
}
