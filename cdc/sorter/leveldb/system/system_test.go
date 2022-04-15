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

package system

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/sorter/leveldb"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"github.com/pingcap/tiflow/pkg/actor"
	actormsg "github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestSystemStartStop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	sys := NewSystem(t.TempDir(), 1, cfg)
	require.Nil(t, sys.Start(ctx))
	require.Nil(t, sys.Stop())

	// Close it again.
	require.Nil(t, sys.Stop())
	// Start a closed system.
	require.Error(t, sys.Start(ctx))
}

func TestSystemStopUnstarted(t *testing.T) {
	t.Parallel()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	sys := NewSystem(t.TempDir(), 1, cfg)
	require.Nil(t, sys.Stop())
}

func TestCollectMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 2

	sys := NewSystem(t.TempDir(), 1, cfg)
	require.Nil(t, sys.Start(ctx))
	collectMetrics(sys.dbs)
	require.Nil(t, sys.Stop())
}

func TestDBActorID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 2

	sys := NewSystem(t.TempDir(), 1, cfg)
	require.Nil(t, sys.Start(ctx))
	id1 := sys.DBActorID(1)
	id2 := sys.DBActorID(1)
	// tableID to actor ID must be deterministic.
	require.Equal(t, id1, id2)
	require.Nil(t, sys.Stop())
}

// Slow actor should not block system.Stop() forever.
func TestSystemStopSlowly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 2

	sys := NewSystem(t.TempDir(), 1, cfg)
	require.Nil(t, sys.Start(ctx))
	msg := message.Task{Test: &message.Test{Sleep: 2 * time.Second}}
	sys.DBRouter.Broadcast(ctx, actormsg.ValueMessage(msg))
	require.Nil(t, sys.Stop())
}

// Mailbox full should not cause system.Stop() being blocked forever.
func TestSystemStopMailboxFull(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 2

	sys := NewSystem(t.TempDir(), 1, cfg)
	require.Nil(t, sys.Start(ctx))
	msg := message.Task{Test: &message.Test{Sleep: 2 * time.Second}}
	sys.DBRouter.Broadcast(ctx, actormsg.ValueMessage(msg))
	for {
		err := sys.DBRouter.Send(actor.ID(1), actormsg.ValueMessage(message.Task{}))
		if err != nil {
			break
		}
	}
	require.Nil(t, sys.Stop())
}

func TestSystemStopWithManyTablesAndFewStragglers(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 8

	sys := NewSystem(t.TempDir(), 1, cfg)
	require.Nil(t, sys.Start(ctx))

	ss := make([]*leveldb.Sorter, 0, 1000)
	scancels := make([]context.CancelFunc, 0, 1000)
	for i := uint64(0); i < 1000; i++ {
		dbActorID := sys.DBActorID(i)
		s, err := leveldb.NewSorter(
			ctx, int64(i), i, sys.DBRouter, dbActorID,
			sys.WriterSystem, sys.WriterRouter,
			sys.ReaderSystem, sys.ReaderRouter,
			sys.CompactScheduler(), cfg)
		require.Nil(t, err)
		ss = append(ss, s)
		sctx, scancel := context.WithCancel(ctx)
		scancels = append(scancels, scancel)
		go func() {
			_ = s.Run(sctx)
		}()
	}
	time.Sleep(500 * time.Millisecond)

	// Close 100 tables ahead.
	for i := 0; i < 100; i++ {
		scancels[i]()
	}
	// 10 stragglers
	for i := 100; i < 110; i++ {
		id := ss[i].ReaderActorID
		sleep := message.Task{Test: &message.Test{Sleep: 2 * time.Second}}
		require.Nil(t, sys.ReaderRouter.SendB(ctx, id, actormsg.ValueMessage(sleep)))
		if i%2 == 0 {
			continue
		}
		// Make it channel full.
		for {
			err := sys.ReaderRouter.Send(id, actormsg.ValueMessage(message.Task{}))
			if err != nil {
				break
			}
		}
	}
	// Close system.
	require.Nil(t, sys.Stop())
}
