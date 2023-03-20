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

package cputil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLightningCheckpointSchema(t *testing.T) {
	t.Parallel()

	cases := []struct {
		task   string
		source string
		schema string
	}{
		{
			task:   "task123",
			source: "source123",
			schema: "task123_2067118723_tidb_lightning_checkpoint",
		},
		{
			task:   "",
			source: "source123",
			schema: "tidb_lightning_checkpoint",
		},
		{
			task:   "task12312312312312312312312312312312",
			source: "task12312312312312312312312312312312",
			schema: "task12312312312312312312312312312312_914146876_tidb_lightning_ch",
		},
	}

	for _, cs := range cases {
		schema := LightningCheckpointSchema(cs.task, cs.source)
		require.Equal(t, cs.schema, schema)
	}
}
