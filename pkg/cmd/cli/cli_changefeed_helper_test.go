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

package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestConfirmLargeDataGap(t *testing.T) {
	t.Parallel()

	currentTs := int64(423482306736160769) // 2021-03-11 17:59:57.547
	startTs := uint64(423450030227042420)  // 2021-03-10 07:47:52.435

	cmd := &cobra.Command{}

	// check start ts more than 1 day before current ts, and type N when confirming
	dir := t.TempDir()
	path := filepath.Join(dir, "confirm.txt")
	err := os.WriteFile(path, []byte("n"), 0o644)
	require.Nil(t, err)
	f, err := os.Open(path)
	require.Nil(t, err)
	stdin := os.Stdin
	os.Stdin = f
	defer func() {
		os.Stdin = stdin
	}()

	err = confirmLargeDataGap(cmd, currentTs, startTs, "test")
	require.Regexp(t, "cli changefeed test", err)

	// check start ts more than 1 day before current ts, and type Y when confirming
	err = os.WriteFile(path, []byte("Y"), 0o644)
	require.Nil(t, err)
	f, err = os.Open(path)
	require.Nil(t, err)
	os.Stdin = f
	err = confirmLargeDataGap(cmd, currentTs, startTs, "test")
	require.Nil(t, err)
}

func TestConfirmIgnoreIneligibleTables(t *testing.T) {
	cmd := &cobra.Command{}

	// check start ts more than 1 day before current ts, and type N when confirming
	dir := t.TempDir()
	path := filepath.Join(dir, "confirm.txt")
	err := os.WriteFile(path, []byte("n"), 0o644)
	require.Nil(t, err)
	f, err := os.Open(path)
	require.Nil(t, err)
	stdin := os.Stdin
	os.Stdin = f
	defer func() {
		os.Stdin = stdin
	}()

	ignore, err := confirmIgnoreIneligibleTables(cmd)
	require.Regexp(t, "cli changefeed create", err)
	require.False(t, ignore)

	// check start ts more than 1 day before current ts, and type Y when confirming
	err = os.WriteFile(path, []byte("Y"), 0o644)
	require.Nil(t, err)
	f, err = os.Open(path)
	require.Nil(t, err)
	os.Stdin = f
	ignore, err = confirmIgnoreIneligibleTables(cmd)
	require.Nil(t, err)
	require.True(t, ignore)
}
