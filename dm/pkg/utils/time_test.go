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

package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseStartTime(t *testing.T) {
	var err error
	_, err = ParseStartTime("2006-01-02T15:04:05")
	require.NoError(t, err)
	_, err = ParseStartTime("2006-01-02 15:04:05")
	require.NoError(t, err)
	_, err = ParseStartTime("15:04:05")
	require.Error(t, err)
}
