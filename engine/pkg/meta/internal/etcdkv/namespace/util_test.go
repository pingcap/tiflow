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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// [reference]: https://github.com/etcd-io/etcd/blob/aa75fd08509db3aea8939cdad44e1ee9b8157b8c/client/v3/namespace/util_test.go

package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrefixInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pfx string
		key []byte
		end []byte

		wKey []byte
		wEnd []byte
	}{
		// single key
		{
			pfx: "pfx/",
			key: []byte("a"),

			wKey: []byte("pfx/a"),
		},
		// range
		{
			pfx: "pfx/",
			key: []byte("abc"),
			end: []byte("def"),

			wKey: []byte("pfx/abc"),
			wEnd: []byte("pfx/def"),
		},
		// one-sided range
		{
			pfx: "pfx/",
			key: []byte("abc"),
			end: []byte{0},

			wKey: []byte("pfx/abc"),
			wEnd: []byte("pfx0"),
		},
		// one-sided range, end of keyspace
		{
			pfx: "\xff\xff",
			key: []byte("abc"),
			end: []byte{0},

			wKey: []byte("\xff\xffabc"),
			wEnd: []byte{0},
		},
	}
	for _, tt := range tests {
		pfxKey, pfxEnd := prefixInterval(tt.pfx, tt.key, tt.end)
		require.Equal(t, pfxKey, tt.wKey)
		require.Equal(t, pfxEnd, tt.wEnd)
	}
}
