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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStartAndStopSystem(t *testing.T) {
	t.Parallel()

	s := NewSystem()
	s.Start(context.TODO())
	s.Stop()
}

func TestActorID(t *testing.T) {
	sys := NewSystem()
	var ids [10000]uint64
	group := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		group.Add(1)
		go func(idx int) {
			for j := 0; j < 100; j++ {
				id := sys.ActorID()
				ids[idx*100+j] = uint64(id)
			}
			group.Done()
		}(i)
	}
	group.Wait()
	idMap := make(map[uint64]struct{})
	for _, id := range ids {
		_, ok := idMap[id]
		require.False(t, ok)
		idMap[id] = struct{}{}
	}
}
