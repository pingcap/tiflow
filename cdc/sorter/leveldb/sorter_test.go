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
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/stretchr/testify/require"
)

func newTestSorter(name string, capacity int) (Sorter, actor.Mailbox) {
	mb := actor.NewMailbox(1, capacity)
	router := actor.NewRouter(name)
	router.InsertMailbox4Test(mb.ID(), mb)

	s := Sorter{
		common: common{
			dbRouter:  router,
			dbActorID: mb.ID(),
			errCh:     make(chan error, 1),
		},
		writerRouter:  router,
		writerActorID: mb.ID(),
		readerRouter:  router,
		readerActorID: mb.ID(),
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
		}, task.SorterTask)
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
		}, task.SorterTask)
}

func TestCleanupFunc(t *testing.T) {
	t.Parallel()

	s, mb := newTestSorter(t.Name(), 1)

	fn := s.CleanupFunc()
	require.Nil(t, fn(context.Background()))
	task, ok := mb.Receive()
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
		}, task.SorterTask)
}
