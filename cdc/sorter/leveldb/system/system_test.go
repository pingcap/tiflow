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

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestSystemStartStop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 1

	sys := NewSystem(t.TempDir(), cfg)
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

	sys := NewSystem(t.TempDir(), cfg)
	require.Nil(t, sys.Stop())
}

func TestCollectMetrics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 2

	sys := NewSystem(t.TempDir(), cfg)
	require.Nil(t, sys.Start(ctx))
	collectMetrics(sys.dbs, "")
	require.Nil(t, sys.Stop())
}

func TestActorID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := config.GetDefaultServerConfig().Clone().Debug.DB
	cfg.Count = 2

	sys := NewSystem(t.TempDir(), cfg)
	require.Nil(t, sys.Start(ctx))
	id1 := sys.ActorID(1)
	id2 := sys.ActorID(1)
	// tableID to actor ID must be deterministic.
	require.Equal(t, id1, id2)
	require.Nil(t, sys.Stop())
}
