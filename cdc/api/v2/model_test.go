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

package v2

import (
	"testing"

	parserModel "github.com/pingcap/tidb/parser/model"
	filter "github.com/pingcap/tidb/util/table-filter"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestToAPIReplicaConfig(t *testing.T) {
	cfg := config.GetDefaultReplicaConfig()
	cfg.EnableOldValue = false
	cfg.CheckGCSafePoint = false
	cfg.Sink = &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{
				Matcher:        []string{"a", "b", "c"},
				DispatcherRule: "",
				PartitionRule:  "rule",
				TopicRule:      "topic",
			},
		},
		Protocol: "aaa",
		ColumnSelectors: []*config.ColumnSelector{
			{
				Matcher: []string{"a", "b", "c"},
				Columns: []string{"a", "b"},
			},
		},
		SchemaRegistry: "bbb",
	}
	cfg.Consistent = &config.ConsistentConfig{
		Level:             "1",
		MaxLogSize:        99,
		FlushIntervalInMs: 10,
		Storage:           "s3",
	}
	cfg.Filter = &config.FilterConfig{
		Rules: []string{"a", "b", "c"},
		MySQLReplicationRules: &filter.MySQLReplicationRules{
			DoTables: []*filter.Table{{
				Schema: "test",
				Name:   "test",
			}},
			DoDBs: []string{"a", "b"},
			IgnoreTables: []*filter.Table{
				{
					Schema: "test",
					Name:   "test",
				},
			},
			IgnoreDBs: []string{"a", "b"},
		},
		IgnoreTxnStartTs: []uint64{1, 2, 3},
		DDLAllowlist: []parserModel.ActionType{
			parserModel.ActionType(2),
		},
	}
	cfg2 := ToAPIReplicaConfig(cfg).ToInternalReplicaConfig()
	require.Equal(t, "", cfg2.Sink.DispatchRules[0].DispatcherRule)
	cfg.Sink.DispatchRules[0].DispatcherRule = ""
	require.Equal(t, cfg, cfg2)
}

func TestChangefeedInfoClone(t *testing.T) {
	cf1 := &ChangeFeedInfo{}
	cf1.UpstreamID = 1
	cf2, err := cf1.Clone()
	require.Nil(t, err)
	require.Equal(t, cf1, cf2)
	cf2.UpstreamID = 2
	require.Equal(t, uint64(1), cf1.UpstreamID)
}
