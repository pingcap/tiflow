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
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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
		Delimiter:            ",",
		Quote:                "\"",
		NullString:           `\N`,
		IncludeCommitTs:      true,
		BinaryEncodingMethod: BinaryEncodingBase64,
	}
	conf.Sink.Terminator = ""
	conf.Sink.DateSeparator = "month"
	conf.Sink.EnablePartitionSeparator = true
	conf.Sink.AdvanceTimeoutInSec = DefaultAdvanceTimeoutInSec

	conf.Sink.KafkaConfig = &KafkaConfig{
		LargeMessageHandle: &LargeMessageHandleConfig{
			LargeMessageHandleOption: LargeMessageHandleOptionHandleKeyOnly,
		},
	}

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

	sinkURL, err := url.Parse("blackhole://xxx?protocol=canal")
	require.NoError(t, err)
	require.NoError(t, conf.ValidateAndAdjust(sinkURL))

	// Incorrect sink configuration.
	conf = GetDefaultReplicaConfig()
	conf.Sink.Protocol = "canal"
	conf.EnableOldValue = false

	err = conf.ValidateAndAdjust(sinkURL)
	require.NoError(t, err)
	require.True(t, conf.EnableOldValue)

	conf = GetDefaultReplicaConfig()
	conf.Sink.DispatchRules = []*DispatchRule{
		{Matcher: []string{"a.b"}, DispatcherRule: "d1", PartitionRule: "r1"},
	}
	err = conf.ValidateAndAdjust(sinkURL)
	require.Regexp(t, ".*dispatcher and partition cannot be configured both.*", err)

	// Correct sink configuration.
	conf = GetDefaultReplicaConfig()
	conf.Sink.DispatchRules = []*DispatchRule{
		{Matcher: []string{"a.b"}, DispatcherRule: "d1"},
		{Matcher: []string{"a.c"}, PartitionRule: "p1"},
		{Matcher: []string{"a.d"}},
	}
	err = conf.ValidateAndAdjust(sinkURL)
	require.Nil(t, err)
	rules := conf.Sink.DispatchRules
	require.Equal(t, "d1", rules[0].PartitionRule)
	require.Equal(t, "p1", rules[1].PartitionRule)
	require.Equal(t, "", rules[2].PartitionRule)

	// Test memory quota can be adjusted
	conf = GetDefaultReplicaConfig()
	conf.MemoryQuota = 0
	err = conf.ValidateAndAdjust(sinkURL)
	require.NoError(t, err)
	require.Equal(t, uint64(DefaultChangefeedMemoryQuota), conf.MemoryQuota)

	conf.MemoryQuota = uint64(1024)
	err = conf.ValidateAndAdjust(sinkURL)
	require.NoError(t, err)
	require.Equal(t, uint64(1024), conf.MemoryQuota)
}

func TestValidateAndAdjust(t *testing.T) {
	cfg := GetDefaultReplicaConfig()
	require.False(t, cfg.EnableSyncPoint)

	sinkURL, err := url.Parse("blackhole://")
	require.NoError(t, err)

	require.NoError(t, cfg.ValidateAndAdjust(sinkURL))

	cfg.EnableSyncPoint = true
	require.NoError(t, cfg.ValidateAndAdjust(sinkURL))

	cfg.SyncPointInterval = time.Second * 29
	require.Error(t, cfg.ValidateAndAdjust(sinkURL))

	cfg.SyncPointInterval = time.Second * 30
	cfg.SyncPointRetention = time.Minute * 10
	require.Error(t, cfg.ValidateAndAdjust(sinkURL))

	cfg.Sink.EncoderConcurrency = -1
	require.Error(t, cfg.ValidateAndAdjust(sinkURL))

	// changefeed error stuck duration is less than 30 minutes
	cfg = GetDefaultReplicaConfig()
	duration := minChangeFeedErrorStuckDuration - time.Second*1
	cfg.ChangefeedErrorStuckDuration = duration
	err = cfg.ValidateAndAdjust(sinkURL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "The ChangefeedErrorStuckDuration")
	duration = minChangeFeedErrorStuckDuration
	cfg.ChangefeedErrorStuckDuration = duration
	require.NoError(t, cfg.ValidateAndAdjust(sinkURL))
}

func TestAdjustEnableOldValueAndVerifyForceReplicate(t *testing.T) {
	t.Parallel()

	config := GetDefaultReplicaConfig()
	config.EnableOldValue = false

	// mysql sink, do not adjust enable-old-value
	sinkURI, err := url.Parse("mysql://")
	require.NoError(t, err)
	err = config.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
	require.NoError(t, err)
	require.False(t, config.EnableOldValue)

	// mysql sink, `enable-old-value` false, `force-replicate` true, should return error
	config.ForceReplicate = true
	err = config.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
	require.Error(t, cerror.ErrOldValueNotEnabled, err)

	// canal, `enable-old-value` false, `force-replicate` false, no error, `enable-old-value` adjust to true
	config.ForceReplicate = false
	config.EnableOldValue = false
	// canal require old value enabled
	sinkURI, err = url.Parse("kafka://127.0.0.1:9092/test?protocol=canal")
	require.NoError(t, err)

	err = config.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
	require.NoError(t, err)
	require.True(t, config.EnableOldValue)

	// canal, `force-replicate` true, `enable-old-value` true, no error
	config.ForceReplicate = true
	config.EnableOldValue = true
	err = config.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
	require.NoError(t, err)
	require.True(t, config.ForceReplicate)
	require.True(t, config.EnableOldValue)

	// avro, `enable-old-value` false, `force-replicate` false, no error
	config.ForceReplicate = false
	config.EnableOldValue = false
	sinkURI, err = url.Parse("kafka://127.0.0.1:9092/test?protocol=avro")
	require.NoError(t, err)

	err = config.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
	require.NoError(t, err)
	require.False(t, config.EnableOldValue)

	// avro, `enable-old-value` true, no error, set to false. no matter `force-replicate`
	config.EnableOldValue = true
	config.ForceReplicate = true
	err = config.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
	require.NoError(t, err)
	require.False(t, config.EnableOldValue)

	// csv, `enable-old-value` false, `force-replicate` false, no error
	config.EnableOldValue = false
	config.ForceReplicate = false
	sinkURI, err = url.Parse("s3://xxx/yyy?protocol=csv")
	require.NoError(t, err)

	err = config.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
	require.NoError(t, err)
	require.False(t, config.EnableOldValue)

	// csv, `enable-old-value` true, no error, set to false. no matter `force-replicate`
	config.EnableOldValue = true
	config.ForceReplicate = true
	err = config.AdjustEnableOldValueAndVerifyForceReplicate(sinkURI)
	require.NoError(t, err)
	require.False(t, config.EnableOldValue)
}

func TestMaskSensitiveData(t *testing.T) {
	config := ReplicaConfig{
		Sink:       nil,
		Consistent: nil,
	}
	config.MaskSensitiveData()
	require.Nil(t, config.Sink)
	require.Nil(t, config.Consistent)
	config.Sink = &SinkConfig{}
	config.Sink.KafkaConfig = &KafkaConfig{
		SASLOAuthTokenURL:     aws.String("http://abc.com?password=bacd"),
		SASLOAuthClientSecret: aws.String("bacd"),
	}
	config.Sink.SchemaRegistry = "http://abc.com?password=bacd"
	config.Consistent = &ConsistentConfig{
		Storage: "http://abc.com?password=bacd",
	}
	config.MaskSensitiveData()
	require.Equal(t, "http://abc.com?password=xxxxx", config.Sink.SchemaRegistry)
	require.Equal(t, "http://abc.com?password=xxxxx", config.Consistent.Storage)
	require.Equal(t, "http://abc.com?password=xxxxx", *config.Sink.KafkaConfig.SASLOAuthTokenURL)
	require.Equal(t, "******", *config.Sink.KafkaConfig.SASLOAuthClientSecret)
}
