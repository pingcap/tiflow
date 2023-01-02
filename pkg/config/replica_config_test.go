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

package config

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func mustIndentJSON(t *testing.T, j string) string {
	var buf bytes.Buffer
	err := json.Indent(&buf, []byte(j), "", "  ")
	require.Nil(t, err)
	return buf.String()
}

func TestReplicaConfigMarshal(t *testing.T) {
	t.Parallel()
	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	conf.Sink.Protocol = "open-protocol"
	conf.Sink.ColumnSelectors = []*ColumnSelector{
		{
			Matcher: []string{"1.1"},
			Columns: []string{"a", "b"},
		},
	}
	conf.Sink.CSVConfig = &CSVConfig{
		Delimiter:       ",",
		Quote:           "\"",
		NullString:      `\N`,
		IncludeCommitTs: true,
	}
	conf.Sink.Terminator = ""
	conf.Sink.DateSeparator = "month"
	conf.Sink.EnablePartitionSeparator = true

	b, err := conf.Marshal()
	require.Nil(t, err)
	require.JSONEq(t, testCfgTestReplicaConfigMarshal1, mustIndentJSON(t, b))
	conf2 := new(ReplicaConfig)
	err = conf2.UnmarshalJSON([]byte(testCfgTestReplicaConfigMarshal2))
	require.Nil(t, err)
	require.Equal(t, conf, conf2)
}

func TestReplicaConfigClone(t *testing.T) {
	t.Parallel()
	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	conf2 := conf.Clone()
	require.Equal(t, conf, conf2)
	conf2.Mounter.WorkerNum = 4
	require.Equal(t, 3, conf.Mounter.WorkerNum)
}

func TestReplicaConfigOutDated(t *testing.T) {
	t.Parallel()
	conf2 := new(ReplicaConfig)
	err := conf2.UnmarshalJSON([]byte(testCfgTestReplicaConfigOutDated))
	require.Nil(t, err)

	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	conf.Sink.Protocol = "open-protocol"
	conf.Sink.DispatchRules = []*DispatchRule{
		{Matcher: []string{"a.b"}, DispatcherRule: "r1"},
		{Matcher: []string{"a.c"}, DispatcherRule: "r2"},
		{Matcher: []string{"a.d"}, DispatcherRule: "r2"},
	}
	conf.Sink.TxnAtomicity = unknownTxnAtomicity
	conf.Sink.DateSeparator = ""
	conf.Sink.CSVConfig = nil
	require.Equal(t, conf, conf2)
}

func TestReplicaConfigValidate(t *testing.T) {
	t.Parallel()
	conf := GetDefaultReplicaConfig()
	require.Nil(t, conf.ValidateAndAdjust(nil))

	// Incorrect sink configuration.
	conf = GetDefaultReplicaConfig()
	conf.Sink.Protocol = "canal"
	conf.EnableOldValue = false
	require.Regexp(t, ".*canal protocol requires old value to be enabled.*",
		conf.ValidateAndAdjust(nil))

	conf = GetDefaultReplicaConfig()
	conf.Sink.DispatchRules = []*DispatchRule{
		{Matcher: []string{"a.b"}, DispatcherRule: "d1", PartitionRule: "r1"},
	}
	require.Regexp(t, ".*dispatcher and partition cannot be configured both.*",
		conf.ValidateAndAdjust(nil))

	// Correct sink configuration.
	conf = GetDefaultReplicaConfig()
	conf.Sink.DispatchRules = []*DispatchRule{
		{Matcher: []string{"a.b"}, DispatcherRule: "d1"},
		{Matcher: []string{"a.c"}, PartitionRule: "p1"},
		{Matcher: []string{"a.d"}},
	}
	err := conf.ValidateAndAdjust(nil)
	require.Nil(t, err)
	rules := conf.Sink.DispatchRules
	require.Equal(t, "d1", rules[0].PartitionRule)
	require.Equal(t, "p1", rules[1].PartitionRule)
	require.Equal(t, "", rules[2].PartitionRule)

	// Test memory quota can be adjusted
	conf = GetDefaultReplicaConfig()
	conf.MemoryQuota = 0
	err = conf.ValidateAndAdjust(nil)
	require.Nil(t, err)
	require.Equal(t, uint64(DefaultChangefeedMemoryQuota), conf.MemoryQuota)

	conf.MemoryQuota = uint64(1024)
	err = conf.ValidateAndAdjust(nil)
	require.Nil(t, err)
	require.Equal(t, uint64(1024), conf.MemoryQuota)
}

func TestValidateAndAdjust(t *testing.T) {
	cfg := GetDefaultReplicaConfig()
	require.False(t, cfg.EnableSyncPoint)
	require.NoError(t, cfg.ValidateAndAdjust(nil))

	cfg.EnableSyncPoint = true
	require.NoError(t, cfg.ValidateAndAdjust(nil))

	cfg.SyncPointInterval = time.Second * 29
	require.Error(t, cfg.ValidateAndAdjust(nil))

	cfg.SyncPointInterval = time.Second * 30
	cfg.SyncPointRetention = time.Minute * 10
	require.Error(t, cfg.ValidateAndAdjust(nil))

	cfg.Sink.EncoderConcurrency = -1
	require.Error(t, cfg.ValidateAndAdjust(nil))
}
