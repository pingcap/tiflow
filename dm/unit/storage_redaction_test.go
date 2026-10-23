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

package unit

import (
	"errors"
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
)

func TestProcessErrorURLCredentials(t *testing.T) {
	raw := errors.New("AccessDenied: Get https://bucket/path?X-Amz-Signature=private-sig status code: 403 request id: test-id")
	err := terror.ErrDumpUnitRuntime.Delegate(raw, "fail to remove output directory: s3://bucket/path?secret-access-key=private-sk")
	before := err.Error()
	result := NewProcessError(err)
	require.NotContains(t, result.String(), "private-")
	require.Equal(t, int32(32001), result.ErrCode)
	require.Contains(t, result.RawCause, "AccessDenied")
	require.Contains(t, result.RawCause, "status code: 403")
	require.Contains(t, result.RawCause, "request id: test-id")
	require.Equal(t, before, err.Error())
	require.NotContains(t, NewProcessError(raw).Message, "private-")
}
