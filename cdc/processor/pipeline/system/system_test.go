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
	"math"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/stretchr/testify/require"
)

func TestStartAndStopSystem(t *testing.T) {
	t.Parallel()

	s := NewSystem()
	require.Nil(t, s.Start(context.TODO()))
	require.Nil(t, s.Stop())
}

func TestActorID(t *testing.T) {
	sys := NewSystem()
	type table struct {
		changeFeed string
		tableID    model.TableID
	}
	cases := []table{
		{"abc", 1},
		{"", -1},
		{"", 0},
		{"", math.MaxInt64},
		{"afddeefessssssss", math.MaxInt64},
		{"afddeefessssssss", 0},
		{"afddeefessssssss", 1},
	}
	ids := make(map[actor.ID]bool)
	for _, c := range cases {
		id1 := sys.ActorID(c.changeFeed, c.tableID)
		for i := 0; i < 10; i++ {
			require.Equal(t, id1, sys.ActorID(c.changeFeed, c.tableID))
		}
		require.False(t, ids[id1])
		ids[id1] = true
	}
}
