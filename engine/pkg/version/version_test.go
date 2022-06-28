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

package version

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/logutil"
	"github.com/stretchr/testify/require"
)

func TestLogVersion(t *testing.T) {
	t.Parallel()

	noneInfo := `Release Version: None
Git Commit Hash: None
Git Branch: None
UTC Build Time: None
Go Version: None
`

	err := logutil.InitLogger(&logutil.Config{})
	require.Nil(t, err)
	require.Equal(t, noneInfo, GetRawInfo())
	LogVersionInfo()
}
