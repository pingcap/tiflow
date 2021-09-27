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

package flags

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewURLsValue(t *testing.T) {
	t.Parallel()

	cases := []struct {
		url        string
		hostString string
	}{
		{"http://127.0.0.1:2379", "127.0.0.1:2379"},
		{"http://127.0.0.1:2379,http://127.0.0.1:2380", "127.0.0.1:2379,127.0.0.1:2380"},
		{"http://pd-1:2379,http://pd-2:2380", "pd-1:2379,pd-2:2380"},
		{"https://127.0.0.1:2379,https://127.0.0.1:2380", "127.0.0.1:2379,127.0.0.1:2380"},
		// TODO: unix socket not supported now
		// {"unix:///home/tidb/tidb.sock", "/home/tidb/tidb.sock"},
	}

	for _, testCase := range cases {
		urlsValue, err := NewURLsValue(testCase.url)
		require.Nil(t, err)
		hs := urlsValue.HostString()
		require.Equal(t, testCase.hostString, hs)
	}
}

func TestNewURLsValueError(t *testing.T) {
	t.Parallel()

	urls := []string{
		"http:///192.168.199.111:2379",
		"http://192.168.199.111",
		"127.0.0.1:1080",
		"http://192.168.199.112:8080/api/v1",
	}
	for _, url := range urls {
		_, err := NewURLsValue(url)
		require.NotNil(t, err)
	}
}
