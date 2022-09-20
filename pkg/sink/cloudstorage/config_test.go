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

package cloudstorage

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigApply(t *testing.T) {
	expected := NewConfig()
	expected.WorkerCount = 32
	uri := "s3://bucket/prefix?worker-count=32"
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	cfg := NewConfig()
	err = cfg.Apply(context.TODO(), sinkURI)
	require.Nil(t, err)
	require.Equal(t, expected, cfg)
}

func TestVerifySinkURIParams(t *testing.T) {
	testCases := []struct {
		name        string
		uri         string
		expectedErr string
	}{
		{
			name:        "valid sink uri with local/nfs scheme",
			uri:         "file://tmp/test",
			expectedErr: "",
		},
		{
			name:        "valid sink uri with s3 scheme",
			uri:         "s3://bucket/prefix",
			expectedErr: "",
		},
		{
			name:        "valid sink uri with gcs scheme",
			uri:         "gcs://bucket/prefix",
			expectedErr: "",
		},
		{
			name:        "valid sink uri with azblob scheme",
			uri:         "azblob://bucket/prefix",
			expectedErr: "",
		},
		{
			name:        "invalid sink uri with unknown storage scheme",
			uri:         "xxx://tmp/test",
			expectedErr: "can't create cloud storage sink with unsupported scheme",
		},
		{
			name:        "invalid sink uri with worker-count number less than lower limit",
			uri:         "file://tmp/test?worker-count=-1",
			expectedErr: "invalid worker-count -1, which must be greater than 0",
		},
		{
			name:        "invalid sink uri with worker-count number greater than upper limit",
			uri:         "s3://bucket/prefix?worker-count=10000",
			expectedErr: "",
		},
	}

	for _, tc := range testCases {
		sinkURI, err := url.Parse(tc.uri)
		require.Nil(t, err)
		cfg := NewConfig()
		err = cfg.Apply(context.TODO(), sinkURI)
		if tc.expectedErr == "" {
			require.Nil(t, err)
			require.LessOrEqual(t, cfg.WorkerCount, maxWorkerCount)
		} else {
			require.Regexp(t, tc.expectedErr, err)
		}
	}
}
