// Copyright 2019 PingCAP, Inc.
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

package relay

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/dm/pkg/binlog/event"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestStreamer(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval", "return(10000)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// generate an event
	header := &replication.EventHeader{
		Timestamp: uint32(time.Now().Unix()),
		ServerID:  11,
	}
	ev, err := event.GenFormatDescriptionEvent(header, 4)
	require.NoError(t, err)

	// 1. get event and error
	s := newLocalStreamer() // with buffer
	s.ch <- ev
	ev2, err := s.GetEvent(ctx)
	require.NoError(t, err)
	require.Equal(t, ev, ev2)

	// read error
	errIn := errors.New("error use for streamer test 1")
	s.ech <- errIn
	ev2, err = s.GetEvent(ctx)
	require.Equal(t, errIn, err)
	require.Nil(t, ev2)

	// can not get event anymore because got error
	ev2, err = s.GetEvent(ctx)
	require.True(t, terror.ErrNeedSyncAgain.Equal(err))
	require.Nil(t, ev2)

	// 2. close with error
	s = newLocalStreamer()
	errClose := errors.New("error use for streamer test 2")
	s.closeWithError(errClose)
	ev2, err = s.GetEvent(ctx)
	require.Equal(t, errClose, err)
	require.Nil(t, ev2)

	// can not get event anymore
	ev2, err = s.GetEvent(ctx)
	require.True(t, terror.ErrNeedSyncAgain.Equal(err))
	require.Nil(t, ev2)

	// 3. close without error
	s = newLocalStreamer()
	s.close()
	ev2, err = s.GetEvent(ctx)
	require.True(t, terror.ErrSyncClosed.Equal(err))
	require.Nil(t, ev2)

	// can not get event anymore
	ev2, err = s.GetEvent(ctx)
	require.True(t, terror.ErrNeedSyncAgain.Equal(err))
	require.Nil(t, ev2)

	// 4. close with nil error
	s = newLocalStreamer()
	s.closeWithError(nil)
	ev2, err = s.GetEvent(ctx)
	require.True(t, terror.ErrSyncClosed.Equal(err))
	require.Nil(t, ev2)

	// can not get event anymore
	ev2, err = s.GetEvent(ctx)
	require.True(t, terror.ErrNeedSyncAgain.Equal(err))
	require.Nil(t, ev2)
}

func TestHeartbeat(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval", "return(1)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tiflow/dm/relay/SetHeartbeatInterval"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s := newLocalStreamer()
	ev, err := s.GetEvent(ctx)
	require.NoError(t, err)
	require.Equal(t, replication.HEARTBEAT_EVENT, ev.Header.EventType)
}
