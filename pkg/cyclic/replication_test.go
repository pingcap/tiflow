// Copyright 2020 PingCAP, Inc.
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

package cyclic

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	"github.com/stretchr/testify/require"
)

func TestCyclicConfig(t *testing.T) {
	t.Parallel()
	cfg := &config.CyclicConfig{
		Enable:          true,
		ReplicaID:       1,
		FilterReplicaID: []uint64{2, 3},
	}
	cyc := NewCyclic(cfg)
	require.NotNil(t, cyc)
	require.True(t, cyc.Enabled())
	require.Equal(t, cyc.ReplicaID(), uint64(1))
	require.Equal(t, cyc.FilterReplicaID(), []uint64{2, 3})

	cyc = NewCyclic(nil)
	require.Nil(t, cyc)
	cyc = NewCyclic(&config.CyclicConfig{ReplicaID: 0})
	require.Nil(t, cyc)
}

func TestRelaxSQLMode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		oldMode string
		newMode string
	}{
		{"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
		{"ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE,STRICT_TRANS_TABLES", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
		{"STRICT_TRANS_TABLES", ""},
		{"ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE", "ONLY_FULL_GROUP_BY,NO_ZERO_IN_DATE"},
	}

	for _, test := range tests {
		getNew := RelaxSQLMode(test.oldMode)
		require.Equal(t, getNew, test.newMode)
	}
}

func TestIsTablePaired(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tables   []model.TableName
		isParied bool
	}{
		{[]model.TableName{}, true},
		{
			[]model.TableName{{Schema: mark.SchemaName, Table: "repl_mark_1"}},
			true,
		},
		{
			[]model.TableName{{Schema: "a", Table: "a"}},
			false,
		},
		{
			[]model.TableName{
				{Schema: mark.SchemaName, Table: "repl_mark_a_a"},
				{Schema: "a", Table: "a"},
			},
			true,
		},
		{
			[]model.TableName{
				{Schema: mark.SchemaName, Table: "repl_mark_a_a"},
				{Schema: mark.SchemaName, Table: "repl_mark_a_b"},
				{Schema: "a", Table: "a"},
				{Schema: "a", Table: "b"},
			},
			true,
		},
	}

	for _, test := range tests {
		require.Equal(t, IsTablesPaired(test.tables), test.isParied,
			"%v", test)
	}
}
