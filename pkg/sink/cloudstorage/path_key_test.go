// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchemaPathKey(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		path      string
		schemakey SchemaPathKey
		checksum  uint32
	}{
		// Test for database schema path: <schema>/meta/schema_{tableVersion}_{checksum}.json
		{
			path: "test_schema/meta/schema_1_2.json",
			schemakey: SchemaPathKey{
				Schema:       "test_schema",
				Table:        "",
				TableVersion: 1,
			},
			checksum: 2,
		},
		// Test for table schema path: <schema>/<table>/meta/schema_{tableVersion}_{checksum}.json
		{
			path: "test_schema/test_table/meta/schema_11_22.json",
			schemakey: SchemaPathKey{
				Schema:       "test_schema",
				Table:        "test_table",
				TableVersion: 11,
			},
			checksum: 22,
		},
	}
	for _, tc := range testCases {
		var schemaKey SchemaPathKey
		checksum, err := schemaKey.ParseSchemaFilePath(tc.path)
		require.NoError(t, err)
		require.Equal(t, tc.schemakey, schemaKey)
		require.Equal(t, tc.checksum, checksum)
	}
}
