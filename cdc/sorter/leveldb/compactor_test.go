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

package leveldb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/db"
	"github.com/stretchr/testify/require"
)

type mockCompactDB struct {
	db.DB
	compact chan struct{}
}

func (m *mockCompactDB) Compact(start, end []byte) error {
	m.compact <- struct{}{}
	return nil
}

func TestCompactorPoll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	mockDB := mockCompactDB{DB: db, compact: make(chan struct{}, 1)}
	closedWg := new(sync.WaitGroup)
	compactor, _, err := NewCompactActor(1, &mockDB, cfg, closedWg, "")
	require.Nil(t, err)

	closed := !compactor.Poll(ctx, []actormsg.Message{actormsg.TickMessage()})
	require.False(t, closed)
	select {
	case <-time.After(5 * time.Second):
		t.Fatal("Must trigger compact")
	case <-mockDB.compact:
	}

	// Close leveldb.
	closed = !compactor.Poll(ctx, []actormsg.Message{actormsg.StopMessage()})
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestComactorContextCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	db, err := db.OpenLevelDB(ctx, 1, t.TempDir(), cfg)
	require.Nil(t, err)
	closedWg := new(sync.WaitGroup)
	ldb, _, err := NewCompactActor(0, db, cfg, closedWg, "")
	require.Nil(t, err)

	cancel()
	closed := !ldb.Poll(ctx, []actormsg.Message{actormsg.TickMessage()})
	require.True(t, closed)
	closedWg.Wait()
	require.Nil(t, db.Close())
}

func TestScheduleCompact(t *testing.T) {
	t.Parallel()
	router := actor.NewRouter(t.Name())
	mb := actor.NewMailbox(actor.ID(1), 1)
	router.InsertMailbox4Test(mb.ID(), mb)
	compact := NewCompactScheduler(
		router, config.GetDefaultServerConfig().Debug.DB)
	compact.compactThreshold = 2

	// Too few deletion, should not trigger compact.
	require.False(t, compact.maybeCompact(mb.ID(), 1))
	_, ok := mb.Receive()
	require.False(t, ok)
	// Must trigger compact.
	require.True(t, compact.maybeCompact(mb.ID(), 3))
	msg, ok := mb.Receive()
	require.True(t, ok)
	require.EqualValues(t, actormsg.TickMessage(), msg)
}
