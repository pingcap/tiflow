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

package leveldb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func newTestSorter(name string, capacity int) (Sorter, actor.Mailbox[message.Task]) {
	mb := actor.NewMailbox[message.Task](1, capacity)
	router := actor.NewRouter[message.Task](name)
	router.InsertMailbox4Test(mb.ID(), mb)

	s := Sorter{
		common: common{
			dbRouter:  router,
			dbActorID: mb.ID(),
			errCh:     make(chan error, 1),
			closedWg:  &sync.WaitGroup{},
		},
		writerRouter:  router,
		writerActorID: mb.ID(),
		readerRouter:  router,
		ReaderActorID: mb.ID(),
	}
	return s, mb
}

func TestAddEntry(t *testing.T) {
	t.Parallel()

	s, mb := newTestSorter(t.Name(), 1)

	event := model.NewResolvedPolymorphicEvent(0, 1)
	s.AddEntry(context.Background(), event)
	task, ok := mb.Receive()
	require.True(t, ok)
	require.EqualValues(t,
		message.Task{
			UID:        s.uid,
			TableID:    s.tableID,
			InputEvent: event,
		}, task.Value)
}

func TestTryAddEntry(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, mb := newTestSorter(t.Name(), 1)

	event := model.NewResolvedPolymorphicEvent(0, 1)
	sent, err := s.TryAddEntry(ctx, event)
	require.True(t, sent)
	require.Nil(t, err)
	task, ok := mb.Receive()
	require.True(t, ok)
	require.EqualValues(t, event, task.Value.InputEvent)

	sent, err = s.TryAddEntry(ctx, event)
	require.True(t, sent)
	require.Nil(t, err)
	sent, err = s.TryAddEntry(ctx, event)
	require.False(t, sent)
	require.Nil(t, err)
}

func TestOutput(t *testing.T) {
	t.Parallel()

	s, mb := newTestSorter(t.Name(), 1)

	s.Output()
	task, ok := mb.Receive()
	require.True(t, ok)
	require.EqualValues(t,
		message.Task{
			UID:     s.uid,
			TableID: s.tableID,
			ReadTs:  message.ReadTs{},
		}, task.Value)
}

func TestRunAndReportError(t *testing.T) {
	t.Parallel()

	// Run exits with three messages
	cap := 3
	s, mb := newTestSorter(t.Name(), cap)
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.common.reportError(
			"test", errors.ErrLevelDBSorterError.GenWithStackByArgs())
	}()
	require.Error(t, s.Run(context.Background()))

	// Stop writer and reader.
	msg, ok := mb.Receive()
	require.True(t, ok)
	require.EqualValues(t, actormsg.StopMessage[message.Task](), msg)
	msg, ok = mb.Receive()
	require.True(t, ok)
	require.EqualValues(t, actormsg.StopMessage[message.Task](), msg)
	// Cleanup
	msg, ok = mb.Receive()
	require.True(t, ok)
	require.EqualValues(t,
		message.Task{
			UID:     s.uid,
			TableID: s.tableID,
			DeleteReq: &message.DeleteRequest{
				Range: [2][]byte{
					encoding.EncodeTsKey(s.uid, s.tableID, 0),
					encoding.EncodeTsKey(s.uid, s.tableID+1, 0),
				},
			},
		}, msg.Value)

	// No more message.
	msg, ok = mb.Receive()
	require.False(t, ok)

	// Must be nonblock.
	s.common.reportError(
		"test", errors.ErrLevelDBSorterError.GenWithStackByArgs())
	s.common.reportError(
		"test", errors.ErrLevelDBSorterError.GenWithStackByArgs())
}
