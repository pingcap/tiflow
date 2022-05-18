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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServerConfigMarshal(t *testing.T) {
	t.Parallel()
	rawConfig := testCfgTestServerConfigMarshal

	conf := GetDefaultServerConfig()
	conf.Addr = "192.155.22.33:8887"
	conf.Sorter.ChunkSizeLimit = 999
	b, err := conf.Marshal()
	require.Nil(t, err)

	require.Equal(t, rawConfig, mustIndentJSON(t, b))
	conf2 := new(ServerConfig)
	err = conf2.Unmarshal([]byte(rawConfig))
	require.Nil(t, err)
	require.Equal(t, conf, conf2)
}

func TestServerConfigClone(t *testing.T) {
	t.Parallel()
	conf := GetDefaultServerConfig()
	conf.Addr = "192.155.22.33:8887"
	conf.Sorter.ChunkSizeLimit = 999
	conf2 := conf.Clone()
	require.Equal(t, conf, conf2)
	conf.Sorter.ChunkSizeLimit = 99
	require.Equal(t, uint64(99), conf.Sorter.ChunkSizeLimit)
}

func TestServerConfigValidateAndAdjust(t *testing.T) {
	t.Parallel()
	conf := new(ServerConfig)

	require.Regexp(t, ".*empty address", conf.ValidateAndAdjust())
	conf.Addr = "cdc:1234"
	require.Regexp(t, ".*empty GC TTL is not allowed", conf.ValidateAndAdjust())
	conf.GcTTL = 60
	require.Nil(t, conf.ValidateAndAdjust())
	require.Equal(t, conf.Addr, conf.AdvertiseAddr)
	conf.AdvertiseAddr = "advertise:1234"
	require.Nil(t, conf.ValidateAndAdjust())
	require.Equal(t, "cdc:1234", conf.Addr)
	require.Equal(t, "advertise:1234", conf.AdvertiseAddr)
	conf.AdvertiseAddr = "0.0.0.0:1234"
	require.Regexp(t, ".*must be specified.*", conf.ValidateAndAdjust())
	conf.Addr = "0.0.0.0:1234"
	require.Regexp(t, ".*must be specified.*", conf.ValidateAndAdjust())
	conf.AdvertiseAddr = "advertise"
	require.Regexp(t, ".*does not contain a port", conf.ValidateAndAdjust())
	conf.AdvertiseAddr = "advertise:1234"
	conf.PerTableMemoryQuota = 1
	require.Nil(t, conf.ValidateAndAdjust())
	require.EqualValues(t, 1, conf.PerTableMemoryQuota)
	conf.PerTableMemoryQuota = 0
	require.Nil(t, conf.ValidateAndAdjust())
	require.EqualValues(t, GetDefaultServerConfig().PerTableMemoryQuota, conf.PerTableMemoryQuota)
	conf.Debug.Messages.ServerWorkerPoolSize = 0
	require.Nil(t, conf.ValidateAndAdjust())
	require.EqualValues(t, GetDefaultServerConfig().Debug.Messages.ServerWorkerPoolSize, conf.Debug.Messages.ServerWorkerPoolSize)
}

func TestDBConfigValidateAndAdjust(t *testing.T) {
	t.Parallel()
	conf := GetDefaultServerConfig().Clone().Debug.DB

	require.Nil(t, conf.ValidateAndAdjust())
	conf.Compression = "none"
	require.Nil(t, conf.ValidateAndAdjust())
	conf.Compression = "snappy"
	require.Nil(t, conf.ValidateAndAdjust())
	conf.Compression = "invalid"
	require.Error(t, conf.ValidateAndAdjust())
}

func TestKVClientConfigValidateAndAdjust(t *testing.T) {
	t.Parallel()
	conf := GetDefaultServerConfig().Clone().KVClient

	require.Nil(t, conf.ValidateAndAdjust())
	conf.RegionRetryDuration = TomlDuration(time.Second)
	require.Nil(t, conf.ValidateAndAdjust())
	conf.RegionRetryDuration = -TomlDuration(time.Second)
	require.Error(t, conf.ValidateAndAdjust())
}
