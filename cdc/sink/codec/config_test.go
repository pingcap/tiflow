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

package codec

import (
	"net/url"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	c := NewConfig("fake-protocol")
	require.Equal(t, c.protocol, "fake-protocol")
	require.Equal(t, c.maxMessageBytes, config.DefaultMaxMessageBytes)
	require.Equal(t, c.maxBatchSize, defaultMaxBatchSize)
	require.Equal(t, c.enableTiDBExtension, "false")
	require.Equal(t, c.avroRegistry, "")
}

func TestConfigApply(t *testing.T) {
	uri := "kafka://127.0.0.1:9092/abc?protocol=canal-json&enable-tidb-extension=true"
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	protocol := sinkURI.Query().Get("protocol")
	require.Equal(t, protocol, "canal-json")

	c := NewConfig(protocol)
	require.Equal(t, c.protocol, "canal-json")

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
	require.Equal(t, "", errors.Cause(err))

	// invalid protocol
	uri = "kafka://127.0.0.1:9092/abc?protocol=fake-protocol&enable-tidb-extension=true"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)

	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)
	require.True(t, c.enableTiDBExtension)

	err = c.Validate()
	require.Equal(t, cerror.ErrMQCodecInvalidConfig, errors.Cause(err))

	// avro
	uri = "kafka://127.0.0.1:9092/abc?protocol=avro"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)

	protocol = sinkURI.Query().Get("protocol")
	require.Equal(t, protocol, "avro")

	c = NewConfig(protocol)
	require.Equal(t, c.protocol, "avro")

	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)
	require.Equal(t, c.avroRegistry, "")
	// `registry` not set
	err = c.Validate()
	require.Equal(t, cerror.ErrMQCodecInvalidConfig, errors.Cause(err))

	opts["registry"] = "this-is-a-uri"
	err = c.Apply(sinkURI, opts)
	require.Nil(t, err)
	require.Equal(t, c.avroRegistry, "this-is-a-uri")
	err = c.Validate()
	require.Nil(t, err)
}
