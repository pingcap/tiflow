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

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMarshalUnmarshal(t *testing.T) {
	t.Parallel()

	info := &CaptureInfo{
		ID:            "9ff52aca-aea6-4022-8ec4-fbee3f2c7890",
		AdvertiseAddr: "127.0.0.1:8300",
		Version:       "dev",
	}
	expected := `{"id":"9ff52aca-aea6-4022-8ec4-fbee3f2c7890","address":"127.0.0.1:8300",` +
		`"version":"dev","git-hash":"","deploy-path":"","start-timestamp":0}`
	data, err := info.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, string(data))
	decodedInfo := &CaptureInfo{}
	err = decodedInfo.Unmarshal(data)
	require.Nil(t, err)
	require.Equal(t, info, decodedInfo)

	// test marshal and unmarshal all fileds
	info = &CaptureInfo{
		ID:             "9ff52aca-aea6-4022-8ec4-fbee3f2c7890",
		AdvertiseAddr:  "127.0.0.1:8300",
		Version:        "dev",
		GitHash:        "1234567890",
		DeployPath:     "/tmp/tiflow",
		StartTimestamp: 1234567890,
	}
	expected = `{"id":"9ff52aca-aea6-4022-8ec4-fbee3f2c7890","address":"127.0.0.1:8300",` +
		`"version":"dev","git-hash":"1234567890","deploy-path":"/tmp/tiflow","start-timestamp":1234567890}`
	data, err = info.Marshal()
	require.Nil(t, err)
	require.Equal(t, expected, string(data))
	decodedInfo = &CaptureInfo{}
	err = decodedInfo.Unmarshal(data)
	require.Nil(t, err)
	require.Equal(t, info, decodedInfo)
}

func TestListVersionsFromCaptureInfos(t *testing.T) {
	infos := []*CaptureInfo{
		{
			ID:            "9ff52aca-aea6-4022-8ec4-fbee3f2c7891",
			AdvertiseAddr: "127.0.0.1:8300",
			Version:       "dev",
		},
		{
			ID:            "9ff52aca-aea6-4022-8ec4-fbee3f2c7891",
			AdvertiseAddr: "127.0.0.1:8300",
			Version:       "",
		},
	}

	require.ElementsMatch(t, []string{"dev", ""}, ListVersionsFromCaptureInfos(infos))
}
