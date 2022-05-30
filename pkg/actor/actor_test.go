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

package actor

import (
	"context"
	_ "net/http/pprof"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/stretchr/testify/require"
)

// Make sure mailbox implementation follows Mailbox definition.
func testMailbox(t *testing.T, mb Mailbox[int]) {
	// Empty mailbox.
	require.Equal(t, 0, mb.len())
	_, ok := mb.Receive()
	require.False(t, ok)

	// Send and receive.
	err := mb.Send(message.ValueMessage(1))
	require.Nil(t, err)
	require.Equal(t, 1, mb.len())
	msg, ok := mb.Receive()
	require.Equal(t, message.ValueMessage(1), msg)
	require.True(t, ok)

	// Empty mailbox.
	_, ok = mb.Receive()
	require.False(t, ok)

	// Mailbox has a bounded capacity.
	for {
		err = mb.Send(message.ValueMessage(1))
		if err != nil {
			break
		}
	}
	// SendB should be blocked.
	ch := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch <- nil
		ch <- mb.SendB(ctx, message.ValueMessage(2))
	}()
	// Wait for goroutine start.
	<-ch
	select {
	case <-time.After(100 * time.Millisecond):
	case err = <-ch:
		t.Fatalf("must timeout, got error %v", err)
	}
	// Receive unblocks SendB
	msg, ok = mb.Receive()
	require.Equal(t, message.ValueMessage(1), msg)
	require.True(t, ok)
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("must not timeout")
	case err = <-ch:
		require.Nil(t, err)
	}

	// SendB must be aware of context cancel.
	ch = make(chan error)
	go func() {
		ch <- nil
		ch <- mb.SendB(ctx, message.ValueMessage(2))
	}()
	// Wait for goroutine start.
	<-ch
	select {
	case <-time.After(100 * time.Millisecond):
	case err = <-ch:
		t.Fatalf("must timeout, got error %v", err)
	}
	cancel()
	select {
	case <-time.After(100 * time.Millisecond):
		t.Fatal("must not timeout")
	case err = <-ch:
		require.Error(t, err)
	}
}

func TestMailbox(t *testing.T) {
	mb := NewMailbox[int](ID(1), 1)
	testMailbox(t, mb)
}
