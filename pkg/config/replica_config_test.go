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
	"github.com/pingcap/tiflow/pkg/compression"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/util"
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
	conf.Sink.Protocol = util.AddressOf("open-protocol")
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
	conf.Sink.TxnAtomicity = util.AddressOf(unknownTxnAtomicity)
	conf.Sink.DateSeparator = util.AddressOf("month")
	conf.Sink.EnablePartitionSeparator = util.AddressOf(true)
	conf.Sink.EnableKafkaSinkV2 = util.AddressOf(true)
	conf.Scheduler.EnableTableAcrossNodes = true
	conf.Scheduler.RegionThreshold = 100001
	conf.Scheduler.WriteKeyThreshold = 100001

	conf.Sink.OnlyOutputUpdatedColumns = aws.Bool(true)
	conf.Sink.DeleteOnlyOutputHandleKeyColumns = aws.Bool(true)
	conf.Sink.SafeMode = aws.Bool(true)
	conf.Sink.AdvanceTimeoutInSec = util.AddressOf(uint(150))
	conf.Sink.KafkaConfig = &KafkaConfig{
		PartitionNum:                 aws.Int32(1),
		ReplicationFactor:            aws.Int16(1),
		KafkaVersion:                 aws.String("version"),
		MaxMessageBytes:              aws.Int(1),
		Compression:                  aws.String("gzip"),
		KafkaClientID:                aws.String("client-id"),
		AutoCreateTopic:              aws.Bool(true),
		DialTimeout:                  aws.String("1m"),
		WriteTimeout:                 aws.String("1m"),
		ReadTimeout:                  aws.String("1m"),
		RequiredAcks:                 aws.Int(1),
		SASLUser:                     aws.String("user"),
		SASLPassword:                 aws.String("password"),
		SASLMechanism:                aws.String("mechanism"),
		SASLGssAPIAuthType:           aws.String("type"),
		SASLGssAPIKeytabPath:         aws.String("path"),
		SASLGssAPIKerberosConfigPath: aws.String("path"),
		SASLGssAPIServiceName:        aws.String("service"),
		SASLGssAPIUser:               aws.String("user"),
		SASLGssAPIPassword:           aws.String("password"),
		SASLGssAPIRealm:              aws.String("realm"),
		SASLGssAPIDisablePafxfast:    aws.Bool(true),
		EnableTLS:                    aws.Bool(true),
		CA:                           aws.String("ca"),
		Cert:                         aws.String("cert"),
		Key:                          aws.String("key"),
		CodecConfig: &CodecConfig{
			EnableTiDBExtension:            aws.Bool(true),
			MaxBatchSize:                   aws.Int(100000),
			AvroEnableWatermark:            aws.Bool(true),
			AvroDecimalHandlingMode:        aws.String("string"),
			AvroBigintUnsignedHandlingMode: aws.String("string"),
		},
		LargeMessageHandle: &LargeMessageHandleConfig{
			LargeMessageHandleOption: LargeMessageHandleOptionHandleKeyOnly,
		},
		GlueSchemaRegistryConfig: &GlueSchemaRegistryConfig{
			Region:       "region",
			RegistryName: "registry",
		},
	}
	conf.Sink.PulsarConfig = &PulsarConfig{
		PulsarVersion:           aws.String("v2.10.0"),
		AuthenticationToken:     aws.String("token"),
		TLSTrustCertsFilePath:   aws.String("TLSTrustCertsFilePath_path"),
		ConnectionTimeout:       NewTimeSec(18),
		OperationTimeout:        NewTimeSec(8),
		BatchingMaxPublishDelay: NewTimeMill(5000),
	}
	conf.Sink.MySQLConfig = &MySQLConfig{
		WorkerCount:                  aws.Int(8),
		MaxTxnRow:                    aws.Int(100000),
		MaxMultiUpdateRowSize:        aws.Int(100000),
		MaxMultiUpdateRowCount:       aws.Int(100000),
		TiDBTxnMode:                  aws.String("pessimistic"),
		SSLCa:                        aws.String("ca"),
		SSLCert:                      aws.String("cert"),
		SSLKey:                       aws.String("key"),
		TimeZone:                     aws.String("UTC"),
		WriteTimeout:                 aws.String("1m"),
		ReadTimeout:                  aws.String("1m"),
		Timeout:                      aws.String("1m"),
		EnableBatchDML:               aws.Bool(true),
		EnableMultiStatement:         aws.Bool(true),
		EnableCachePreparedStatement: aws.Bool(true),
	}
	conf.Sink.CloudStorageConfig = &CloudStorageConfig{
		WorkerCount:    aws.Int(8),
		FlushInterval:  aws.String("1m"),
		FileSize:       aws.Int(1024),
		OutputColumnID: aws.Bool(false),
	}
	conf.Sink.OpenProtocol = &OpenProtocolConfig{
		OutputOldValue: true,
	}

	b, err := conf.Marshal()
	require.NoError(t, err)
	b = mustIndentJSON(t, b)
	require.JSONEq(t, testCfgTestReplicaConfigMarshal1, b)

	conf2 := new(ReplicaConfig)
	err = conf2.UnmarshalJSON([]byte(testCfgTestReplicaConfigMarshal2))
	require.NoError(t, err)
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
	require.NoError(t, err)

	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	conf.Sink.Protocol = util.AddressOf("open-protocol")
	conf.Sink.DispatchRules = []*DispatchRule{
		{Matcher: []string{"a.b"}, DispatcherRule: "r1"},
		{Matcher: []string{"a.c"}, DispatcherRule: "r2"},
		{Matcher: []string{"a.d"}, DispatcherRule: "r2"},
	}
	conf.Sink.CSVConfig = nil
	require.Equal(t, conf, conf2)
}

func TestReplicaConfigValidate(t *testing.T) {
	t.Parallel()
	conf := GetDefaultReplicaConfig()

	sinkURL, err := url.Parse("blackhole://")
	require.NoError(t, err)
	require.NoError(t, conf.ValidateAndAdjust(sinkURL))

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
	require.NoError(t, err)
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

	conf.Scheduler = &ChangefeedSchedulerConfig{
		EnableTableAcrossNodes: true,
		RegionThreshold:        -1,
	}
	err = conf.ValidateAndAdjust(sinkURL)
	require.Error(t, err)
}

func TestValidateIntegrity(t *testing.T) {
	sinkURL, err := url.Parse("kafka://topic?protocol=avro")
	require.NoError(t, err)

	cfg := GetDefaultReplicaConfig()
	cfg.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	cfg.Sink.ColumnSelectors = []*ColumnSelector{
		{
			Matcher: []string{"a.b"}, Columns: []string{"c"},
		},
	}

	err = cfg.ValidateAndAdjust(sinkURL)
	require.ErrorIs(t, err, cerror.ErrInvalidReplicaConfig)
}

func TestValidateAndAdjust(t *testing.T) {
	cfg := GetDefaultReplicaConfig()

	require.False(t, util.GetOrZero(cfg.EnableSyncPoint))
	sinkURL, err := url.Parse("blackhole://")
	require.NoError(t, err)

	require.NoError(t, cfg.ValidateAndAdjust(sinkURL))

	cfg.EnableSyncPoint = util.AddressOf(true)
	require.NoError(t, cfg.ValidateAndAdjust(sinkURL))

	cfg.SyncPointInterval = util.AddressOf(time.Second * 29)
	require.Error(t, cfg.ValidateAndAdjust(sinkURL))

	cfg.SyncPointInterval = util.AddressOf(time.Second * 30)
	cfg.SyncPointRetention = util.AddressOf(time.Minute * 10)
	require.Error(t, cfg.ValidateAndAdjust(sinkURL))

	cfg.Sink.EncoderConcurrency = util.AddressOf(-1)
	require.Error(t, cfg.ValidateAndAdjust(sinkURL))

	cfg = GetDefaultReplicaConfig()
	cfg.Scheduler = nil
	require.Nil(t, cfg.ValidateAndAdjust(sinkURL))
	require.False(t, cfg.Scheduler.EnableTableAcrossNodes)

	// enable the checksum verification, but use blackhole sink
	cfg = GetDefaultReplicaConfig()
	cfg.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness
	require.NoError(t, cfg.ValidateAndAdjust(sinkURL))
	require.Equal(t, integrity.CheckLevelNone, cfg.Integrity.IntegrityCheckLevel)

	// changefeed error stuck duration is less than 30 minutes
	cfg = GetDefaultReplicaConfig()
	duration := minChangeFeedErrorStuckDuration - time.Second*1
	cfg.ChangefeedErrorStuckDuration = &duration
	err = cfg.ValidateAndAdjust(sinkURL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "The ChangefeedErrorStuckDuration")
	duration = minChangeFeedErrorStuckDuration
	cfg.ChangefeedErrorStuckDuration = &duration
	require.NoError(t, cfg.ValidateAndAdjust(sinkURL))
}

func TestIsSinkCompatibleWithSpanReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		uri        string
		compatible bool
	}{
		{
			name:       "MySQL URI",
			uri:        "mysql://root:111@foo.bar:3306/",
			compatible: false,
		},
		{
			name:       "TiDB URI",
			uri:        "tidb://root:111@foo.bar:3306/",
			compatible: false,
		},
		{
			name:       "MySQL URI",
			uri:        "mysql+ssl://root:111@foo.bar:3306/",
			compatible: false,
		},
		{
			name:       "TiDB URI",
			uri:        "tidb+ssl://root:111@foo.bar:3306/",
			compatible: false,
		},
		{
			name:       "Kafka URI",
			uri:        "kafka://foo.bar:3306/topic",
			compatible: true,
		},
		{
			name:       "Kafka URI",
			uri:        "kafka+ssl://foo.bar:3306/topic",
			compatible: true,
		},
		{
			name:       "Blackhole URI",
			uri:        "blackhole://foo.bar:3306/topic",
			compatible: true,
		},
		{
			name:       "Unknown URI",
			uri:        "unknown://foo.bar:3306",
			compatible: false,
		},
	}

	for _, tt := range tests {
		u, e := url.Parse(tt.uri)
		require.Nil(t, e)
		compatible := isSinkCompatibleWithSpanReplication(u)
		require.Equal(t, compatible, tt.compatible, tt.name)
	}
}

func TestValidateAndAdjustLargeMessageHandle(t *testing.T) {
	cfg := GetDefaultReplicaConfig()
	cfg.Sink.KafkaConfig = &KafkaConfig{
		LargeMessageHandle: NewDefaultLargeMessageHandleConfig(),
	}
	cfg.Sink.KafkaConfig.LargeMessageHandle.LargeMessageHandleOption = ""
	cfg.Sink.KafkaConfig.LargeMessageHandle.LargeMessageHandleCompression = ""

	rawURL := "kafka://127.0.0.1:9092/canal-json-test?protocol=canal-json&enable-tidb-extension=true"
	sinkURL, err := url.Parse(rawURL)
	require.NoError(t, err)

	err = cfg.ValidateAndAdjust(sinkURL)
	require.NoError(t, err)

	require.Equal(t, LargeMessageHandleOptionNone, cfg.Sink.KafkaConfig.LargeMessageHandle.LargeMessageHandleOption)
	require.Equal(t, compression.None, cfg.Sink.KafkaConfig.LargeMessageHandle.LargeMessageHandleCompression)
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
		SASLPassword:          aws.String("bacd"),
		SASLGssAPIPassword:    aws.String("bacd"),
		Key:                   aws.String("bacd"),
		GlueSchemaRegistryConfig: &GlueSchemaRegistryConfig{
			AccessKey:       "abc",
			SecretAccessKey: "def",
			Token:           "aaa",
		},
	}
	config.Sink.SchemaRegistry = aws.String("http://abc.com?password=bacd")
	config.Consistent = &ConsistentConfig{
		Storage: "http://abc.com?password=bacd",
	}
	config.MaskSensitiveData()
	require.Equal(t, "http://abc.com?password=xxxxx", *config.Sink.SchemaRegistry)
	require.Equal(t, "http://abc.com?password=xxxxx", config.Consistent.Storage)
	require.Equal(t, "http://abc.com?password=xxxxx", *config.Sink.KafkaConfig.SASLOAuthTokenURL)
	require.Equal(t, "******", *config.Sink.KafkaConfig.SASLOAuthClientSecret)
	require.Equal(t, "******", *config.Sink.KafkaConfig.Key)
	require.Equal(t, "******", *config.Sink.KafkaConfig.SASLPassword)
	require.Equal(t, "******", *config.Sink.KafkaConfig.SASLGssAPIPassword)
	require.Equal(t, "******", config.Sink.KafkaConfig.GlueSchemaRegistryConfig.SecretAccessKey)
	require.Equal(t, "******", config.Sink.KafkaConfig.GlueSchemaRegistryConfig.Token)
	require.Equal(t, "******", config.Sink.KafkaConfig.GlueSchemaRegistryConfig.AccessKey)
}
