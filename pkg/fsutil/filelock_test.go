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

package fsutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBasicCase tests the basic scenarios of using FileLock and all operations are expected to be successful
func TestBasicCase(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/test_lock"

	fileLock, err := NewFileLock(path)
	require.Nil(t, err)
	defer fileLock.Close() //nolint:errcheck

	fileLockCheck, err := NewFileLock(path)
	require.Nil(t, err)
	defer fileLockCheck.Close() //nolint:errcheck

	isLocked, err := fileLock.IsLocked()
	require.Nil(t, err)
	require.False(t, isLocked)

	err = fileLock.Lock()
	require.Nil(t, err)

	isLocked, err = fileLockCheck.IsLocked()
	require.Nil(t, err)
	require.True(t, isLocked)

	err = fileLock.Unlock()
	require.Nil(t, err)

	isLocked, err = fileLockCheck.IsLocked()
	require.Nil(t, err)
	require.False(t, isLocked)

	isLocked, err = fileLock.IsLocked()
	require.Nil(t, err)
	require.False(t, isLocked)
}

// TestBadPath tests the case where the file path is not accessible
func TestBadPath(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/bad_path/bad_file"

	_, err := NewFileLock(path)
	require.Regexp(t, ".*no such file.*", err)
}

// TestBadPath tests the case where the file is locked twice
// We do not expect this to happen in TiCDC.
func TestDuplicateLocking(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/test_lock"

	fileLock, err := NewFileLock(path)
	require.Nil(t, err)
	defer fileLock.Close() //nolint:errcheck

	fileLock2, err := NewFileLock(path)
	require.Nil(t, err)
	defer fileLock2.Close() //nolint:errcheck

	err = fileLock.Lock()
	require.Nil(t, err)

	err = fileLock2.Lock()
	require.Regexp(t, ".*ErrConflictingFileLocks.*", err)
}
