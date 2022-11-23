// Copyright 2021 PingCAP, Inc.
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

package binlog

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadSortedBinlogFromDir(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	filenames := []string{
		"bin.000001", "bin.000002", "bin.100000", "bin.100001", "bin.1000000", "bin.1000001", "bin.999999", "relay.meta",
	}
	expected := []string{
		"bin.000001", "bin.000002", "bin.100000", "bin.100001", "bin.999999", "bin.1000000", "bin.1000001",
	}
	for _, f := range filenames {
		require.Nil(t, os.WriteFile(filepath.Join(dir, f), nil, 0o600))
	}
	ret, err := ReadSortedBinlogFromDir(dir)
	require.Nil(t, err)
	require.Equal(t, expected, ret)
}
