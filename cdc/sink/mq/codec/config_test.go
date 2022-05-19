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

package codec

import (
	"net/url"
	"testing"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	t.Parallel()

	c := NewConfig(config.ProtocolDefault)
	require.Equal(t, config.ProtocolDefault, c.protocol)
	require.Equal(t, config.DefaultMaxMessageBytes, c.maxMessageBytes)
	require.Equal(t, defaultMaxBatchSize, c.maxBatchSize)
	require.Equal(t, false, c.enableTiDBExtension)
	require.Equal(t, "precise", c.avroDecimalHandlingMode)
	require.Equal(t, "long", c.avroBigintUnsignedHandlingMode)
	require.Equal(t, "", c.avroSchemaRegistry)
}

func TestConfigApplyValidate(t *testing.T) {
	t.Parallel()

	// enable-tidb-extension
	uri := "kafka://127.0.0.1:9092/abc?protocol=canal-json&enable-tidb-extension=true"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	protocol := sinkURI.Query().Get("protocol")
	require.Equal(t, "canal-json", protocol)

	var p config.Protocol
	err = p.FromString(protocol)
	require.NoError(t, err)

	c := NewConfig(p)
	require.Equal(t, config.ProtocolCanalJSON, c.protocol)

	replicaConfig := config.GetDefaultReplicaConfig()
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.True(t, c.enableTiDBExtension)

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
	err = p.FromString(protocol)
	require.NoError(t, err)

	c = NewConfig(p)
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.True(t, c.enableTiDBExtension)

	err = c.Validate()
	require.ErrorContains(t, err, "enable-tidb-extension only supports canal-json/avro protocol")

	// avro
	uri = "kafka://127.0.0.1:9092/abc?protocol=avro"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	protocol = sinkURI.Query().Get("protocol")
	require.Equal(t, "avro", protocol)
	err = p.FromString(protocol)
	require.NoError(t, err)
	c = NewConfig(p)
	require.Equal(t, config.ProtocolAvro, c.protocol)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "", c.avroSchemaRegistry)
	// `schema-registry` not set
	err = c.Validate()
	require.ErrorContains(t, err, `Avro protocol requires parameter "schema-registry"`)

	replicaConfig.Sink.SchemaRegistry = "this-is-a-uri"
	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "this-is-a-uri", c.avroSchemaRegistry)
	err = c.Validate()
	require.NoError(t, err)

	// avro-decimal-handling-mode
	c = NewConfig(config.ProtocolAvro)
	require.Equal(t, "precise", c.avroDecimalHandlingMode)

	uri = "kafka://127.0.0.1:9092/abc?protocol=avro&avro-decimal-handling-mode=string"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "string", c.avroDecimalHandlingMode)

	err = c.Validate()
	require.NoError(t, err)

	uri = "kafka://127.0.0.1:9092/abc?protocol=avro&avro-decimal-handling-mode=invalid"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "invalid", c.avroDecimalHandlingMode)

	err = c.Validate()
	require.ErrorContains(
		t,
		err,
		`avro-decimal-handling-mode value could only be "string" or "precise"`,
	)

	// avro-bigint-unsigned-handling-mode
	c = NewConfig(config.ProtocolAvro)
	require.Equal(t, "long", c.avroBigintUnsignedHandlingMode)

	uri = "kafka://127.0.0.1:9092/abc?protocol=avro&avro-bigint-unsigned-handling-mode=string"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "string", c.avroBigintUnsignedHandlingMode)

	err = c.Validate()
	require.NoError(t, err)

	uri = "kafka://127.0.0.1:9092/abc?protocol=avro&avro-bigint-unsigned-handling-mode=invalid"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)

	err = c.Apply(sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, "invalid", c.avroBigintUnsignedHandlingMode)

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
}
