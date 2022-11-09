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

package metadata

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
	"github.com/stretchr/testify/require"
)

func TestUnitStateStore(t *testing.T) {
	t.Parallel()

	s := NewUnitStateStore(mock.NewMetaMock())
	ctx := context.Background()
	state, err := s.Get(ctx)
	require.EqualError(t, err, "state not found")
	require.Nil(t, state)

	state = &UnitState{
		FinishedUnitStatus: map[string][]*FinishedTaskStatus{
			"task": {&FinishedTaskStatus{
				Status: json.RawMessage("null"),
			}},
		},
		CurrentUnitStatus: map[string]*UnitStatus{
			"task": {},
		},
	}
	err = s.Put(ctx, state)
	require.NoError(t, err)

	state2, err2 := s.Get(ctx)
	require.NoError(t, err2)
	require.Equal(t, state, state2)

	err = s.Delete(ctx)
	require.NoError(t, err)

	state, err = s.Get(ctx)
	require.EqualError(t, err, "state not found")
	require.Nil(t, state)
}
