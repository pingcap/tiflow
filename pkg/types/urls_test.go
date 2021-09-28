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

package types

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestURLs(t *testing.T) {
	t.Parallel()
	urlstrs := []string{
		"http://www.google.com:12306",
		"http://192.168.199.111:1080",
		"http://hostname:9000",
	}
	sorted := []string{
		"http://192.168.199.111:1080",
		"http://hostname:9000",
		"http://www.google.com:12306",
	}

	urls, err := NewURLs(urlstrs)
	require.Nil(t, err)
	require.Equal(t, strings.Join(sorted, ","), urls.String())
}

func TestBadURLs(t *testing.T) {
	t.Parallel()
	badurls := [][]string{
		{"http://192.168.199.111"},
		{"127.0.0.1:1080"},
		{"http://192.168.199.112:8080/api/v1"},
	}

	for _, badurl := range badurls {
		_, err := NewURLs(badurl)
		require.NotNil(t, err)
	}
}
