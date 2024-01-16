// Copyright 2023 PingCAP, Inc.
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

	"github.com/stretchr/testify/require"
)

func TestDefaultCDCV2Config(t *testing.T) {
	defaultCDCV2 := defaultServerConfig.Debug.CDCV2
	require.NotNil(t, defaultCDCV2)
	require.False(t, defaultCDCV2.Enable)
}

func TestCDCV2ValidateAndAdjust(t *testing.T) {
	cdcV2 := &CDCV2{
		Enable:          false,
		MetaStoreConfig: MetaStoreConfiguration{},
	}
	require.Nil(t, cdcV2.ValidateAndAdjust())
	cdcV2.Enable = true
	require.NotNil(t, cdcV2.ValidateAndAdjust())
	cdcV2.MetaStoreConfig.URI = "http://127.0.0.1"
	require.NotNil(t, cdcV2.ValidateAndAdjust())
	cdcV2.MetaStoreConfig.URI = "mysql://127.0.0.1"
	require.Nil(t, cdcV2.ValidateAndAdjust())
}

func TestGenDSN(t *testing.T) {
	storeConfig := &MetaStoreConfiguration{
		URI: "mysql://root:abcd@127.0.0.1:4000/cdc?a=c&timeout=1m",
	}
	dsn, err := storeConfig.GenDSN()
	require.Nil(t, err)
	require.Equal(t, "root", dsn.User)
	require.Equal(t, "abcd", dsn.Passwd)
	require.Equal(t, "127.0.0.1:4000", dsn.Addr)
	require.Equal(t, "cdc", dsn.DBName)
	require.Equal(t, "true", dsn.Params["parseTime"])
	require.Equal(t, "1m", dsn.Params["timeout"])
	require.Equal(t, "c", dsn.Params["a"])
}
