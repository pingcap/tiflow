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
// See the License for the specific language governing permissions and
// limitations under the License.

package openapi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperateTaskTableStructureRequestSchema(t *testing.T) {
	spec, err := GetSwagger()
	require.NoError(t, err)

	schema := spec.Components.Schemas["OperateTaskTableStructureRequest"].Value
	require.NotContains(t, schema.Required, "sql_content")
	require.NotContains(t, schema.Required, "flush")
	require.NotContains(t, schema.Required, "schema_source")
	require.NotContains(t, schema.Required, "sync")
	require.Equal(t, true, schema.Properties["flush"].Value.Default)
	require.Equal(t, "sql", schema.Properties["schema_source"].Value.Default)
	require.ElementsMatch(
		t,
		[]interface{}{"sql", "upstream", "downstream"},
		schema.Properties["schema_source"].Value.Enum,
	)
	require.Equal(t, true, schema.Properties["sync"].Value.Default)
}
