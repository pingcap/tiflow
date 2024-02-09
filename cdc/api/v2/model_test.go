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
	"encoding/json"
	"strings"
	"testing"
	"time"

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

// note: this is api published default value, not change it
var defaultAPIConfig = &ReplicaConfig{
	MemoryQuota:        config.DefaultChangefeedMemoryQuota,
	CaseSensitive:      false,
	CheckGCSafePoint:   true,
	BDRMode:            util.AddressOf(false),
	EnableSyncPoint:    util.AddressOf(false),
	EnableTableMonitor: util.AddressOf(false),
	SyncPointInterval:  &JSONDuration{10 * time.Minute},
	SyncPointRetention: &JSONDuration{24 * time.Hour},
	Filter: &FilterConfig{
		Rules: []string{"*.*"},
	},
	Mounter: &MounterConfig{
		WorkerNum: 16,
	},
	Sink: &SinkConfig{
		CSVConfig: &CSVConfig{
			Quote:                string(config.DoubleQuoteChar),
			Delimiter:            config.Comma,
			NullString:           config.NULL,
			BinaryEncodingMethod: config.BinaryEncodingBase64,
		},
		EncoderConcurrency:               util.AddressOf(config.DefaultEncoderGroupConcurrency),
		Terminator:                       util.AddressOf(config.CRLF),
		DateSeparator:                    util.AddressOf(config.DateSeparatorDay.String()),
		EnablePartitionSeparator:         util.AddressOf(true),
		EnableKafkaSinkV2:                util.AddressOf(false),
		OnlyOutputUpdatedColumns:         util.AddressOf(false),
		DeleteOnlyOutputHandleKeyColumns: util.AddressOf(false),
		ContentCompatible:                util.AddressOf(false),
		AdvanceTimeoutInSec:              util.AddressOf(uint(150)),
		SendBootstrapIntervalInSec:       util.AddressOf(int64(120)),
		SendBootstrapInMsgCount:          util.AddressOf(int32(10000)),
		SendBootstrapToAllPartition:      util.AddressOf(true),
		DebeziumDisableSchema:            util.AddressOf(false),
	},
	Consistent: &ConsistentConfig{
		Level:                 "none",
		MaxLogSize:            64,
		FlushIntervalInMs:     redo.DefaultFlushIntervalInMs,
		MetaFlushIntervalInMs: redo.DefaultMetaFlushIntervalInMs,
		EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
		FlushWorkerNum:        redo.DefaultFlushWorkerNum,
		Storage:               "",
		UseFileBackend:        false,
		MemoryUsage: &ConsistentMemoryUsage{
			MemoryQuotaPercentage: 50,
		},
	},
	Scheduler: &ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: config.GetDefaultReplicaConfig().
			Scheduler.EnableTableAcrossNodes,
		RegionThreshold: config.GetDefaultReplicaConfig().
			Scheduler.RegionThreshold,
		WriteKeyThreshold: config.GetDefaultReplicaConfig().
			Scheduler.WriteKeyThreshold,
	},
	Integrity: &IntegrityConfig{
		IntegrityCheckLevel:   config.GetDefaultReplicaConfig().Integrity.IntegrityCheckLevel,
		CorruptionHandleLevel: config.GetDefaultReplicaConfig().Integrity.CorruptionHandleLevel,
	},
	ChangefeedErrorStuckDuration: &JSONDuration{*config.
		GetDefaultReplicaConfig().ChangefeedErrorStuckDuration},
	SQLMode:      config.GetDefaultReplicaConfig().SQLMode,
	SyncedStatus: (*SyncedStatusConfig)(config.GetDefaultReplicaConfig().SyncedStatus),
}

func TestDefaultReplicaConfig(t *testing.T) {
	t.Parallel()
	require.Equal(t, defaultAPIConfig, GetDefaultReplicaConfig())
	cfg := GetDefaultReplicaConfig()
	require.NotNil(t, cfg.Scheduler)
	require.NotNil(t, cfg.Integrity)
	cfg2 := cfg.toInternalReplicaConfigWithOriginConfig(&config.ReplicaConfig{})
	require.Equal(t, config.GetDefaultReplicaConfig(), cfg2)
	cfg3 := ToAPIReplicaConfig(config.GetDefaultReplicaConfig())
	require.Equal(t, cfg, cfg3)
}

func TestToAPIReplicaConfig(t *testing.T) {
	cfg := config.GetDefaultReplicaConfig()
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
		Protocol: util.AddressOf("aaa"),
		ColumnSelectors: []*config.ColumnSelector{
			{
				Matcher: []string{"a", "b", "c"},
				Columns: []string{"a", "b"},
			},
		},
		SchemaRegistry: util.AddressOf("bbb"),
		TxnAtomicity:   util.AddressOf(config.AtomicityLevel("aa")),
	}
	cfg.Consistent = &config.ConsistentConfig{
		Level:             "1",
		MaxLogSize:        99,
		FlushIntervalInMs: 10,
		Storage:           "s3",
	}
	cfg.Filter = &config.FilterConfig{
		Rules:            []string{"a", "b", "c"},
		IgnoreTxnStartTs: []uint64{1, 2, 3},
		EventFilters: []*config.EventFilterRule{{
			Matcher:                  []string{"test.t1", "test.t2"},
			IgnoreEvent:              []bf.EventType{bf.AllDML, bf.AllDDL, bf.AlterTable},
			IgnoreSQL:                []string{"^DROP TABLE", "ADD COLUMN"},
			IgnoreInsertValueExpr:    "c >= 0",
			IgnoreUpdateNewValueExpr: "age <= 55",
			IgnoreUpdateOldValueExpr: "age >= 84",
			IgnoreDeleteValueExpr:    "age > 20",
		}},
	}
	cfg.Mounter = &config.MounterConfig{WorkerNum: 11}
	cfg.Scheduler = &config.ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: true, RegionThreshold: 10001, WriteKeyThreshold: 10001,
	}
	cfg2 := ToAPIReplicaConfig(cfg).ToInternalReplicaConfig()
	require.Equal(t, "", cfg2.Sink.DispatchRules[0].DispatcherRule)
	cfg.Sink.DispatchRules[0].DispatcherRule = ""
	require.Equal(t, cfg, cfg2)
	cfgJSON, err := json.Marshal(ToAPIReplicaConfig(cfg))
	require.Nil(t, err)
	require.False(t, strings.Contains(string(cfgJSON), "-"))
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

func TestToCredential(t *testing.T) {
	t.Parallel()

	pdCfg := &PDConfig{
		PDAddrs:       nil,
		CAPath:        "test-CAPath",
		CertPath:      "test-CertPath",
		KeyPath:       "test-KeyPath",
		CertAllowedCN: nil,
	}

	credential := pdCfg.toCredential()
	require.Equal(t, pdCfg.CertPath, credential.CertPath)
	require.Equal(t, pdCfg.CAPath, credential.CAPath)
	require.Equal(t, pdCfg.KeyPath, credential.KeyPath)
	require.Equal(t, len(credential.CertAllowedCN), 0)

	pdCfg.CertAllowedCN = []string{"test-CertAllowedCN"}
	require.Equal(t, len(credential.CertAllowedCN), 0) // deep copy

	credential = pdCfg.toCredential()
	require.Equal(t, pdCfg.CertPath, credential.CertPath)
	require.Equal(t, pdCfg.CAPath, credential.CAPath)
	require.Equal(t, pdCfg.KeyPath, credential.KeyPath)
	require.Equal(t, len(credential.CertAllowedCN), 1)
	require.Equal(t, credential.CertAllowedCN[0], pdCfg.CertAllowedCN[0])
}

func TestEventFilterRuleConvert(t *testing.T) {
	cases := []struct {
		inRule  *config.EventFilterRule
		apiRule EventFilterRule
	}{
		{
			inRule: &config.EventFilterRule{
				Matcher:                  []string{"test.t1", "test.t2"},
				IgnoreEvent:              []bf.EventType{bf.AllDML, bf.AllDDL, bf.AlterTable},
				IgnoreSQL:                []string{"^DROP TABLE", "ADD COLUMN"},
				IgnoreInsertValueExpr:    "c >= 0",
				IgnoreUpdateNewValueExpr: "age <= 55",
				IgnoreUpdateOldValueExpr: "age >= 84",
				IgnoreDeleteValueExpr:    "age > 20",
			},
			apiRule: EventFilterRule{
				Matcher:                  []string{"test.t1", "test.t2"},
				IgnoreEvent:              []string{"all dml", "all ddl", "alter table"},
				IgnoreSQL:                []string{"^DROP TABLE", "ADD COLUMN"},
				IgnoreInsertValueExpr:    "c >= 0",
				IgnoreUpdateNewValueExpr: "age <= 55",
				IgnoreUpdateOldValueExpr: "age >= 84",
				IgnoreDeleteValueExpr:    "age > 20",
			},
		},
	}
	for _, c := range cases {
		require.Equal(t, c.apiRule, ToAPIEventFilterRule(c.inRule))
		require.Equal(t, c.inRule, c.apiRule.ToInternalEventFilterRule())
	}
}

func TestMarshalChangefeedCommonInfo(t *testing.T) {
	t.Parallel()

	cfInfo := &ChangefeedCommonInfo{
		ID:        "test-id",
		FeedState: model.StatePending,
	}

	cfInfoJSON, err := json.Marshal(cfInfo)
	require.Nil(t, err)
	require.False(t, strings.Contains(string(cfInfoJSON), "pending"))
	require.True(t, strings.Contains(string(cfInfoJSON), "warning"))

	cfInfo = &ChangefeedCommonInfo{
		ID:        "test-id",
		FeedState: model.StateUnInitialized,
	}

	cfInfoJSON, err = json.Marshal(cfInfo)
	require.Nil(t, err)
	require.True(t, strings.Contains(string(cfInfoJSON), "normal"))
}
