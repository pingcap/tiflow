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

package election_test

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/election"
	"github.com/stretchr/testify/require"
)

func TestMemberClone(t *testing.T) {
	t.Parallel()

	m := &election.Member{
		ID:   "id",
		Name: "name",
	}
	m2 := m.Clone()
	require.False(t, m == m2)
	require.Equal(t, m, m2)
}

func TestRecordClone(t *testing.T) {
	t.Parallel()

	r := &election.Record{
		LeaderID: "leader",
		Members: []*election.Member{
			{
				ID:   "id",
				Name: "name",
			},
		},
	}
	r2 := r.Clone()
	require.False(t, r == r2)
	require.Equal(t, r, r2)
	r.Members[0] = nil
	require.NotEqual(t, r, r2)
}

func TestRecordFindMember(t *testing.T) {
	t.Parallel()

	r := &election.Record{
		LeaderID: "leader",
		Members: []*election.Member{
			{
				ID:   "id1",
				Name: "name1",
			},
			{
				ID:   "id2",
				Name: "name2",
			},
			{
				ID:   "id3",
				Name: "name3",
			},
		},
	}
	_, ok := r.FindMember("id")
	require.False(t, ok)

	m, ok := r.FindMember("id2")
	require.True(t, ok)
	require.True(t, m == r.Members[1])
}
