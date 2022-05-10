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

	"github.com/pingcap/tidb/util/timeutil"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	t.Parallel()
	c := NewConfig(config.ProtocolDefault, timeutil.SystemLocation())
	require.Equal(t, c.protocol, config.ProtocolDefault)
	require.Equal(t, c.maxMessageBytes, config.DefaultMaxMessageBytes)
	require.Equal(t, c.maxBatchSize, defaultMaxBatchSize)
	require.Equal(t, c.enableTiDBExtension, false)
	require.Equal(t, c.avroRegistry, "")
}

func TestConfigApplyValidate(t *testing.T) {
	t.Parallel()
	uri := "kafka://127.0.0.1:9092/abc?protocol=canal-json&enable-tidb-extension=true"
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	protocol := sinkURI.Query().Get("protocol")
	require.Equal(t, protocol, "canal-json")

	var p config.Protocol
	err = p.FromString(protocol)
	require.Nil(t, err)

	c := NewConfig(p, timeutil.SystemLocation())
	require.Equal(t, c.protocol, config.ProtocolCanalJSON)

	opts := make(map[string]string)
	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)
	require.True(t, c.enableTiDBExtension)

	err = c.Validate()
	require.Nil(t, err)

	// illegal enable-tidb-extension
	uri = "kafka://127.0.0.1:9092/abc?protocol=canal-json&enable-tidb-extension=a"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	err = c.Apply(sinkURI, opts)
	require.Error(t, err, "invalid syntax")

	// Use enable-tidb-extension on other protocols
	uri = "kafka://127.0.0.1:9092/abc?protocol=open-protocol&enable-tidb-extension=true"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)

	protocol = sinkURI.Query().Get("protocol")
	err = p.FromString(protocol)
	require.Nil(t, err)

	c = NewConfig(p, timeutil.SystemLocation())
	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)
	require.True(t, c.enableTiDBExtension)

	err = c.Validate()
	require.Error(t, err, "enable-tidb-extension only support canal-json protocol")

	// avro
	uri = "kafka://127.0.0.1:9092/abc?protocol=avro"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)

	protocol = sinkURI.Query().Get("protocol")
	require.Equal(t, protocol, "avro")
	err = p.FromString(protocol)
	require.Nil(t, err)
	c = NewConfig(p, timeutil.SystemLocation())
	require.Equal(t, c.protocol, config.ProtocolAvro)

	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)
	require.Equal(t, c.avroRegistry, "")
	// `registry` not set
	err = c.Validate()
	require.Error(t, err, `Avro protocol requires parameter "registry"`)

	opts["registry"] = "this-is-a-uri"
	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)
	require.Equal(t, c.avroRegistry, "this-is-a-uri")
	err = c.Validate()
	require.Nil(t, err)

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	err = c.Apply(sinkURI, opts)
	require.Error(t, err, "invalid syntax")

	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=-1"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)

	err = c.Validate()
	require.Error(t, err, cerror.ErrMQCodecInvalidConfig)

	// Illegal max-batch-size
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-batch-size=a"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	err = c.Apply(sinkURI, opts)
	require.Error(t, err, "invalid syntax")

	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-batch-size=-1"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)

	c = NewConfig(config.ProtocolOpen, timeutil.SystemLocation())
	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)

	err = c.Validate()
	require.Error(t, err, cerror.ErrMQCodecInvalidConfig)
}
