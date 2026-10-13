// Copyright 2026 PingCAP, Inc.
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

package splitter

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/stretchr/testify/require"
)

func TestPrintChunkInfoRedaction(t *testing.T) {
	buffer := captureSplitterLogs(t)
	testfailpoint.Enable(t, "github.com/pingcap/tiflow/sync_diff_inspector/splitter/print-chunk-info", "return(true)")
	testCases := []struct {
		name        string
		newIterator func(*chunk.Range) ChunkIterator
	}{
		{
			name: "random",
			newIterator: func(c *chunk.Range) ChunkIterator {
				return &RandomIterator{chunks: []*chunk.Range{c}}
			},
		},
		{
			name: "limit",
			newIterator: func(c *chunk.Range) ChunkIterator {
				chunks := make(chan *chunk.Range, 1)
				chunks <- c
				return &LimitIterator{chunksCh: chunks}
			},
		},
		{
			name: "bucket",
			newIterator: func(c *chunk.Range) ChunkIterator {
				return &BucketIterator{chunks: []*chunk.Range{c}}
			},
		},
	}
	for _, redact := range []bool{false, true} {
		for _, tc := range testCases {
			t.Run(fmt.Sprintf("%s/redact=%t", tc.name, redact), func(t *testing.T) {
				dmlog.SetRedactLog(redact)
				buffer.Reset()
				c := chunk.NewChunkRange(nil)
				c.Update("a", "private-lower-a", "private-upper-a", true, true)
				c.Update("b", "private-lower-b", "private-upper-b", true, true)
				original := c.Clone()

				result, err := tc.newIterator(c).Next()
				require.NoError(t, err)
				require.Same(t, c, result)
				require.Equal(t, original, result)
				entry := findSplitterLog(t, buffer, fmt.Sprintf("failpoint print-chunk-info injected (%s splitter)", tc.name))
				require.Equal(t, c.Index.ToString(), entry["indexCode"])
				if redact {
					require.Equal(t, "?", entry["lowerBounds"])
					require.Equal(t, "?", entry["upperBounds"])
					require.NotContains(t, buffer.String(), "private-")
				} else {
					require.Equal(t, []any{"private-lower-a", "private-lower-b"}, entry["lowerBounds"])
					require.Equal(t, []any{"private-upper-a", "private-upper-b"}, entry["upperBounds"])
				}
			})
		}
	}
}
