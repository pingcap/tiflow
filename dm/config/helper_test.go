// Copyright 2023 PingCAP, Inc.
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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskModeHasFunction(t *testing.T) {
	require.True(t, HasDump(ModeAll))
	require.True(t, HasLoad(ModeAll))
	require.True(t, HasSync(ModeAll))

	require.True(t, HasDump(ModeFull))
	require.True(t, HasLoad(ModeFull))
	require.False(t, HasSync(ModeFull))

	require.False(t, HasDump(ModeIncrement))
	require.False(t, HasLoad(ModeIncrement))
	require.True(t, HasSync(ModeIncrement))

	require.True(t, HasDump(ModeDump))
	require.False(t, HasLoad(ModeDump))
	require.False(t, HasSync(ModeDump))

	require.False(t, HasDump(ModeLoadSync))
	require.True(t, HasLoad(ModeLoadSync))
	require.True(t, HasSync(ModeLoadSync))
}
