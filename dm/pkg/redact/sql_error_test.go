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

package redact

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestStorageErrorPreservesSQLRedaction(t *testing.T) {
	previous := log.IsRedactLogEnabled()
	t.Cleanup(func() { log.SetRedactLog(previous) })
	err := terror.ErrParseSQL.Delegate(errors.New("line 1 column 19 near private-value"),
		"ALTER TABLE t ADD v VARCHAR(100) DEFAULT 'private-value'")
	log.SetRedactLog(true)
	field := Error(err)
	require.NotContains(t, field.String, "private-value")
	require.Contains(t, field.String, "VARCHAR(100)")
	require.Contains(t, field.String, "line 1 column 19")
	require.Contains(t, err.Error(), "private-value")
	log.SetRedactLog(false)
	require.Contains(t, Error(err).String, "private-value")
}
