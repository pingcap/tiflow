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
	"time"

	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestConfigApply(t *testing.T) {
	expected := NewConfig()
	expected.WorkerCount = 32
	expected.FlushInterval = 10 * time.Second
	expected.FileSize = 16 * 1024 * 1024
	expected.DateSeparator = config.DateSeparatorNone.String()
	uri := "s3://bucket/prefix?worker-count=32&flush-interval=10s&file-size=16777216"
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	cfg := NewConfig()
	err = cfg.Apply(context.TODO(), sinkURI, config.GetDefaultReplicaConfig())
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
			name:        "sink uri with valid scheme, worker-count, flush-interval and file-size",
			uri:         "s3://bucket/prefix?worker-count=64&flush-interval=1m30s&file-size=33554432",
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
			expectedErr: "invalid worker-count -1, it must be greater than 0",
		},
		{
			name:        "invalid sink uri with worker-count number greater than upper limit",
			uri:         "s3://bucket/prefix?worker-count=10000",
			expectedErr: "",
		},
		{
			name:        "invalid sink uri with flush-interval less than lower limit",
			uri:         "s3://bucket/prefix?flush-interval=-10s",
			expectedErr: "",
		},
		{
			name:        "invalid sink uri with flush-interval greater than upper limit",
			uri:         "s3://bucket/prefix?flush=interval=1h",
			expectedErr: "",
		},
		{
			name:        "invalid sink uri with file-size less than lower limit",
			uri:         "s3://bucket/prefix?file-size=1024",
			expectedErr: "",
		},
		{
			name:        "invalid sink uri with file-size greater than upper limit",
			uri:         "s3://bucket/prefix?file-size=1073741824",
			expectedErr: "",
		},
	}

	for _, tc := range testCases {
		sinkURI, err := url.Parse(tc.uri)
		require.Nil(t, err)
		cfg := NewConfig()
		err = cfg.Apply(context.TODO(), sinkURI, config.GetDefaultReplicaConfig())
		if tc.expectedErr == "" {
			require.Nil(t, err)
			require.LessOrEqual(t, cfg.WorkerCount, maxWorkerCount)
			require.LessOrEqual(t, cfg.FlushInterval, maxFlushInterval)
			require.LessOrEqual(t, cfg.FileSize, maxFileSize)
		} else {
			require.Regexp(t, tc.expectedErr, err)
		}
	}
}
