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

package common

import (
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/integrity"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	t.Parallel()

	c := NewConfig(config.ProtocolDefault)
	require.Equal(t, config.ProtocolDefault, c.Protocol)
	require.Equal(t, config.DefaultMaxMessageBytes, c.MaxMessageBytes)
	require.Equal(t, defaultMaxBatchSize, c.MaxBatchSize)
	require.Equal(t, false, c.EnableTiDBExtension)
	require.Equal(t, "precise", c.AvroDecimalHandlingMode)
	require.Equal(t, "long", c.AvroBigintUnsignedHandlingMode)
	require.Equal(t, "", c.AvroConfluentSchemaRegistry)
	require.False(t, c.EnableRowChecksum)
	require.NotNil(t, c.LargeMessageHandle)
}

func TestConfigApplyValidate4EnableRowChecksum(t *testing.T) {
	t.Parallel()

	// enable the row level checksum
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Integrity.IntegrityCheckLevel = integrity.CheckLevelCorrectness

	// avro, all requirement satisfied, should return no error
	replicaConfig.Sink.SchemaRegistry = util.AddressOf("some-schema-registry")

	uri := "kafka://127.0.0.1:9092/abc?protocol=avro&enable-tidb-extension=true&" +
		"avro-decimal-handling-mode=string&avro-bigint-unsigned-handling-mode=string"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	protocol := sinkURI.Query().Get("protocol")
	p, err := config.ParseSinkProtocolFromString(protocol)
	require.NoError(t, err)
	c := NewConfig(p)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)

	err = c.Validate()
	require.NoError(t, err)

	// avo, not all requirement satisfied, return error
	invalidSinkURI := []string{
		"kafka://127.0.0.1:9092/abc?protocol=avro",
		"kafka://127.0.0.1:9092/abc?protocol=avro&enable-tidb-extension=true",

		"kafka://127.0.0.1:9092/abc?protocol=avro&enable-tidb-extension=true&" +
			"avro-decimal-handling-mode=string",

		"kafka://127.0.0.1:9092/abc?protocol=avro&enable-tidb-extension=true&" +
			"avro-bigint-unsigned-handling-mode=string",
	}

	for _, uri := range invalidSinkURI {
		sinkURI, err = url.Parse(uri)
		require.NoError(t, err)

		protocol = sinkURI.Query().Get("protocol")
		p, err = config.ParseSinkProtocolFromString(protocol)
		require.NoError(t, err)
		c = NewConfig(p)

		err = c.Apply(sinkURI, replicaConfig)
		require.NoError(t, err)

		err = c.Validate()
		require.Error(t, err)
	}

	// avro, enable tidb extension and enable watermark event.
	uri = "kafka://127.0.0.1:9092/avro?protocol=avro&enable-tidb-extension=true&avro-enable-watermark=true"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	protocol = sinkURI.Query().Get("protocol")
	p, err = config.ParseSinkProtocolFromString(protocol)
	require.NoError(t, err)
	c = NewConfig(p)

	replicaConfig = config.GetDefaultReplicaConfig()
	replicaConfig.Sink.SchemaRegistry = util.AddressOf("some-schema-registry")
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)

	err = c.Validate()
	require.NoError(t, err)
	require.True(t, c.AvroEnableWatermark)
}

func TestLarMessageHandleNotAllowForceReplicate(t *testing.T) {
	t.Parallel()

	// large-message-handle not set, always no error
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		LargeMessageHandle: config.NewDefaultLargeMessageHandleConfig(),
	}

	uri := "kafka://127.0.0.1:9092/large-message-handle?protocol=canal-json&enable-tidb-extension=true"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	codecConfig := NewConfig(config.ProtocolCanalJSON)
	err = codecConfig.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)

	// force-replicate is set to true, should return error
	replicaConfig.ForceReplicate = true
	err = codecConfig.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)

	replicaConfig.Sink.KafkaConfig.LargeMessageHandle.LargeMessageHandleOption = config.LargeMessageHandleOptionHandleKeyOnly
	err = codecConfig.Apply(sinkURI, replicaConfig)
	require.ErrorIs(t, err, cerror.ErrCodecInvalidConfig)
}

func TestDeleteHandleKeyOnly(t *testing.T) {
	t.Parallel()

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DeleteOnlyOutputHandleKeyColumns = util.AddressOf(true)

	uri := "kafka://127.0.0.1:9092/delete-handle-key-only?protocol=open-protocol"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	codecConfig := NewConfig(config.ProtocolOpen)
	err = codecConfig.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)

	replicaConfig.ForceReplicate = true
	err = codecConfig.Apply(sinkURI, replicaConfig)
	require.ErrorIs(t, err, cerror.ErrCodecInvalidConfig)
}

func TestAvroForceReplicate(t *testing.T) {
	t.Parallel()

	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.ForceReplicate = true

	uri := "kafka://127.0.0.1:9092/abc?protocol=avro"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	codecConfig := NewConfig(config.ProtocolAvro)
	err = codecConfig.Apply(sinkURI, replicaConfig)
	require.ErrorIs(t, err, cerror.ErrCodecInvalidConfig)
}

func TestConfigApplyValidate(t *testing.T) {
	t.Parallel()

	// enable-tidb-extension
	uri := "kafka://127.0.0.1:9092/abc?protocol=canal-json&enable-tidb-extension=true"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	protocol := sinkURI.Query().Get("protocol")
	require.Equal(t, "canal-json", protocol)

	p, err := config.ParseSinkProtocolFromString(protocol)
	require.NoError(t, err)

	c := NewConfig(p)
	require.Equal(t, config.ProtocolCanalJSON, c.Protocol)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.True(t, c.EnableTiDBExtension)
	require.False(t, c.DeleteOnlyHandleKeyColumns)

	err = c.Validate()
	require.NoError(t, err)

	uri = "kafka://127.0.0.1:9092/abc?protocol=canal-json&enable-tidb-extension=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	err = c.Apply(sinkURI, replicaConfig)
	require.ErrorContains(t, err, "invalid syntax")

	// Use enable-tidb-extension on other protocols
	uri = "kafka://127.0.0.1:9092/abc?protocol=open-protocol&enable-tidb-extension=true"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	protocol = sinkURI.Query().Get("protocol")
	p, err = config.ParseSinkProtocolFromString(protocol)
	require.NoError(t, err)

	c = NewConfig(p)
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.True(t, c.EnableTiDBExtension)

	err = c.Validate()
	require.NoError(t, err)

	// avro
	uri = "kafka://127.0.0.1:9092/abc?protocol=avro"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	protocol = sinkURI.Query().Get("protocol")
	require.Equal(t, "avro", protocol)
	p, err = config.ParseSinkProtocolFromString(protocol)
	require.NoError(t, err)
	c = NewConfig(p)
	require.Equal(t, config.ProtocolAvro, c.Protocol)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "", c.AvroConfluentSchemaRegistry)
	// `schema-registry` not set
	err = c.Validate()
	require.ErrorContains(t, err, `Avro protocol requires parameter "schema-registry"`)

	replicaConfig.Sink.SchemaRegistry = util.AddressOf("this-is-a-uri")
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "this-is-a-uri", c.AvroConfluentSchemaRegistry)
	err = c.Validate()
	require.NoError(t, err)

	// avro-decimal-handling-mode
	c = NewConfig(config.ProtocolAvro)
	require.Equal(t, "precise", c.AvroDecimalHandlingMode)

	uri = "kafka://127.0.0.1:9092/abc?protocol=avro&avro-decimal-handling-mode=string"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "string", c.AvroDecimalHandlingMode)

	err = c.Validate()
	require.NoError(t, err)

	uri = "kafka://127.0.0.1:9092/abc?protocol=avro&avro-decimal-handling-mode=invalid"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "invalid", c.AvroDecimalHandlingMode)

	err = c.Validate()
	require.ErrorContains(
		t,
		err,
		`avro-decimal-handling-mode value could only be "string" or "precise"`,
	)

	// avro-bigint-unsigned-handling-mode
	c = NewConfig(config.ProtocolAvro)
	require.Equal(t, "long", c.AvroBigintUnsignedHandlingMode)

	uri = "kafka://127.0.0.1:9092/abc?protocol=avro&avro-bigint-unsigned-handling-mode=string"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "string", c.AvroBigintUnsignedHandlingMode)

	err = c.Validate()
	require.NoError(t, err)

	uri = "kafka://127.0.0.1:9092/abc?protocol=avro&avro-bigint-unsigned-handling-mode=invalid"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "invalid", c.AvroBigintUnsignedHandlingMode)

	err = c.Validate()
	require.ErrorContains(
		t,
		err,
		`bigint-unsigned-handling-mode value could only be "long" or "string"`,
	)

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	err = c.Apply(sinkURI, replicaConfig)
	require.ErrorContains(t, err, "invalid syntax")

	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=-1"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	c = NewConfig(config.ProtocolOpen)
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)

	err = c.Validate()
	require.ErrorContains(t, err, "invalid max-message-bytes -1")

	// Illegal max-batch-size
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-batch-size=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	err = c.Apply(sinkURI, replicaConfig)
	require.ErrorContains(t, err, "invalid syntax")

	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-batch-size=-1"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	c = NewConfig(config.ProtocolOpen)
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)

	err = c.Validate()
	require.ErrorContains(t, err, "invalid max-batch-size -1")

	uri = "kafka://127.0.0.1:9092/abc?protocol=open-protocol"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	c = NewConfig(config.ProtocolOpen)
	replicaConfig = config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DeleteOnlyOutputHandleKeyColumns = util.AddressOf(true)
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		LargeMessageHandle: &config.LargeMessageHandleConfig{
			LargeMessageHandleOption: config.LargeMessageHandleOptionHandleKeyOnly,
		},
	}
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.True(t, c.DeleteOnlyHandleKeyColumns)
	require.NotNil(t, c.LargeMessageHandle)
}

func TestMergeConfig(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	uri := "kafka://127.0.0.1:9092/abc?" +
		"protocol=avro&enable-tidb-extension=true&schema-registry=abc&" +
		"only-output-updated-columns=true&avro-enable-watermark=true&" +
		"avro-bigint-unsigned-handling-mode=ab&avro-decimal-handling-mode=cd&" +
		"max-message-bytes=123&max-batch-size=456"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	c := NewConfig(config.ProtocolAvro)
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, true, c.EnableTiDBExtension)
	require.Equal(t, "abc", c.AvroConfluentSchemaRegistry)
	require.True(t, c.OnlyOutputUpdatedColumns)
	require.True(t, c.AvroEnableWatermark)
	require.False(t, c.ContentCompatible)
	require.Equal(t, "ab", c.AvroBigintUnsignedHandlingMode)
	require.Equal(t, "cd", c.AvroDecimalHandlingMode)
	require.Equal(t, 123, c.MaxMessageBytes)
	require.Equal(t, 456, c.MaxBatchSize)

	// test override
	uri = "kafka://127.0.0.1:9092/abc"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	replicaConfig.Sink.OnlyOutputUpdatedColumns = aws.Bool(true)
	replicaConfig.Sink.DeleteOnlyOutputHandleKeyColumns = aws.Bool(true)
	replicaConfig.Sink.SchemaRegistry = util.AddressOf("abc")
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		MaxMessageBytes: aws.Int(123),
		CodecConfig: &config.CodecConfig{
			EnableTiDBExtension:            aws.Bool(true),
			MaxBatchSize:                   aws.Int(456),
			AvroEnableWatermark:            aws.Bool(true),
			AvroBigintUnsignedHandlingMode: aws.String("ab"),
			AvroDecimalHandlingMode:        aws.String("cd"),
			EncodingFormat:                 aws.String("json"),
		},
		LargeMessageHandle: &config.LargeMessageHandleConfig{
			LargeMessageHandleOption: config.LargeMessageHandleOptionHandleKeyOnly,
		},
	}
	c = NewConfig(config.ProtocolAvro)
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, true, c.EnableTiDBExtension)
	require.Equal(t, "abc", c.AvroConfluentSchemaRegistry)
	require.True(t, c.OnlyOutputUpdatedColumns)
	require.True(t, c.AvroEnableWatermark)
	require.False(t, c.ContentCompatible)
	require.Equal(t, "ab", c.AvroBigintUnsignedHandlingMode)
	require.Equal(t, "cd", c.AvroDecimalHandlingMode)
	require.Equal(t, 123, c.MaxMessageBytes)
	require.Equal(t, 456, c.MaxBatchSize)
	require.Equal(t, config.LargeMessageHandleOptionHandleKeyOnly, c.LargeMessageHandle.LargeMessageHandleOption)

	// test override
	uri = "kafka://127.0.0.1:9092/abc?" +
		"protocol=avro&enable-tidb-extension=true&schema-registry=abc&" +
		"only-output-updated-columns=true&avro-enable-watermark=true&" +
		"avro-bigint-unsigned-handling-mode=ab&avro-decimal-handling-mode=cd&" +
		"max-message-bytes=123&max-batch-size=456"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	replicaConfig.Sink.OnlyOutputUpdatedColumns = aws.Bool(false)
	replicaConfig.Sink.DeleteOnlyOutputHandleKeyColumns = aws.Bool(true)
	replicaConfig.Sink.SchemaRegistry = util.AddressOf("abcd")
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		MaxMessageBytes: aws.Int(1233),
		CodecConfig: &config.CodecConfig{
			EnableTiDBExtension:            aws.Bool(false),
			MaxBatchSize:                   aws.Int(222),
			AvroEnableWatermark:            aws.Bool(false),
			AvroBigintUnsignedHandlingMode: aws.String("adb"),
			AvroDecimalHandlingMode:        aws.String("cde"),
			EncodingFormat:                 aws.String("avro"),
		},
		LargeMessageHandle: &config.LargeMessageHandleConfig{
			LargeMessageHandleOption: config.LargeMessageHandleOptionClaimCheck,
			ClaimCheckStorageURI:     "file:///claim-check",
		},
	}
	c = NewConfig(config.ProtocolAvro)
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, true, c.EnableTiDBExtension)
	require.Equal(t, "abc", c.AvroConfluentSchemaRegistry)
	require.True(t, c.OnlyOutputUpdatedColumns)
	require.True(t, c.AvroEnableWatermark)
	require.False(t, c.ContentCompatible)
	require.Equal(t, "ab", c.AvroBigintUnsignedHandlingMode)
	require.Equal(t, "cd", c.AvroDecimalHandlingMode)
	require.Equal(t, 123, c.MaxMessageBytes)
	require.Equal(t, 456, c.MaxBatchSize)
	require.Equal(t, c.LargeMessageHandle.LargeMessageHandleOption, config.LargeMessageHandleOptionClaimCheck)

	replicaConfig = config.GetDefaultReplicaConfig()
	replicaConfig.Sink.ContentCompatible = aws.Bool(true)
	uri = "kafka://127.0.0.1:9092/content-compatible?protocol=canal-json"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	c = NewConfig(config.ProtocolCanalJSON)
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.True(t, c.ContentCompatible)
	require.True(t, c.OnlyOutputUpdatedColumns)
}

func TestApplyConfig4CanalJSON(t *testing.T) {
	uri := "kafka://127.0.0.1:9092/abc?protocol=canal-json&content-compatible=true"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	codecConfig := NewConfig(config.ProtocolCanalJSON)
	err = codecConfig.Apply(sinkURI, config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	require.True(t, codecConfig.ContentCompatible)
	require.True(t, codecConfig.OnlyOutputUpdatedColumns)
}

func TestConfig4Simple(t *testing.T) {
	uri := "kafka://127.0.0.1:9092/abc?protocol=simple"
	sinkURL, err := url.Parse(uri)
	require.NoError(t, err)

	codecConfig := NewConfig(config.ProtocolSimple)
	err = codecConfig.Apply(sinkURL, config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	require.Equal(t, EncodingFormatJSON, codecConfig.EncodingFormat)

	uri = "kafka://127.0.0.1:9092/abc?protocol=simple&encoding-format=avro"
	sinkURL, err = url.Parse(uri)
	require.NoError(t, err)

	codecConfig = NewConfig(config.ProtocolSimple)
	err = codecConfig.Apply(sinkURL, config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	require.Equal(t, EncodingFormatAvro, codecConfig.EncodingFormat)

	uri = "kafka://127.0.0.1:9092/abc?protocol=simple&encoding-format=xxx"
	sinkURL, err = url.Parse(uri)
	require.NoError(t, err)

	codecConfig = NewConfig(config.ProtocolSimple)
	err = codecConfig.Apply(sinkURL, config.GetDefaultReplicaConfig())
	require.ErrorIs(t, err, cerror.ErrCodecInvalidConfig)
}
