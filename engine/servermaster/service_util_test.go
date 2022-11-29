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

package servermaster

import (
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	electionMock "github.com/pingcap/tiflow/engine/pkg/election/mock"
	"github.com/stretchr/testify/require"
)

func TestGenerateNodeID(t *testing.T) {
	const (
		name           = "executor"
		genCount       = 1000
		minUniqueCount = 999 // Only allow 0.1% of collisions.
	)

	ids := make(map[string]struct{})
	for i := 0; i < genCount; i++ {
		id := generateNodeID(name)
		require.True(t, strings.HasPrefix(id, name+"-"))
		ids[id] = struct{}{}
	}

	require.GreaterOrEqual(t, len(ids), minUniqueCount, "too many collisions")
}

func TestFeatureDegrader(t *testing.T) {
	fd := newFeatureDegrader()
	require.False(t, fd.Available("ListExecutors"))
	require.False(t, fd.Available("CreateJob"))
	require.True(t, fd.Available("QueryMetaStore"))
	require.True(t, fd.Available("UnknownAPI"))

	fd.updateExecutorManager(true)
	fd.updateMasterWorkerManager(true)
	require.True(t, fd.Available("ListExecutors"))
	require.True(t, fd.Available("CreateJob"))
}

func TestForwardChecker(t *testing.T) {
	t.Parallel()

	elector := electionMock.NewMockElector(gomock.NewController(t))
	fc := newForwardChecker(elector)
	for method := range leaderOnlyMethods {
		require.True(t, fc.LeaderOnly(method))
	}

	elector.EXPECT().IsLeader().Times(1).Return(true)
	require.True(t, fc.IsLeader())
	elector.EXPECT().IsLeader().Times(1).Return(false)
	require.False(t, fc.IsLeader())
}
