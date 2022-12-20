// Copyright 2022 PingCAP, Inc.
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
package fsutil

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPreAllocate(t *testing.T) {
	f, err := os.CreateTemp("", "preallocate-test")
	defer os.Remove(f.Name())
	require.Nil(t, err)

	size := int64(64 * 1024 * 1024)
	err = PreAllocate(f, size)
	require.Nil(t, err)

	stat, err := f.Stat()
	require.Nil(t, err)
	require.Equal(t, size, stat.Size())
}
