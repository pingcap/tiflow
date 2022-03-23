// Copyright 2020 PingCAP, Inc.
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

package unified

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/fsutil"
	"github.com/stretchr/testify/require"
)

func TestBasicFunction(t *testing.T) {
	dataDir := t.TempDir()
	err := os.MkdirAll(dataDir, 0o755)
	require.Nil(t, err)

	sortDir := filepath.Join(dataDir, config.DefaultSortDir)
	err = os.MkdirAll(sortDir, 0o755)
	require.Nil(t, err)

	conf := config.GetDefaultServerConfig()
	conf.DataDir = dataDir
	conf.Sorter.SortDir = sortDir
	conf.Sorter.MaxMemoryPercentage = 90                       // 90%
	conf.Sorter.MaxMemoryConsumption = 16 * 1024 * 1024 * 1024 // 16G
	config.StoreGlobalServerConfig(conf)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/memoryPressureInjectPoint", "return(100)")
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	backEndPool, err := newBackEndPool(sortDir)
	require.Nil(t, err)
	require.NotNil(t, backEndPool)
	defer backEndPool.terminate()

	backEnd, err := backEndPool.alloc(ctx)
	require.Nil(t, err)
	require.IsType(t, &fileBackEnd{}, backEnd)
	fileName := backEnd.(*fileBackEnd).fileName
	require.NotEqual(t, "", fileName)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/memoryPressureInjectPoint", "return(0)")
	require.Nil(t, err)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/memoryUsageInjectPoint", "return(34359738368)")
	require.Nil(t, err)

	backEnd1, err := backEndPool.alloc(ctx)
	require.Nil(t, err)
	require.IsType(t, &fileBackEnd{}, backEnd1)
	fileName1 := backEnd1.(*fileBackEnd).fileName
	require.NotEqual(t, "", fileName1)
	require.NotEqual(t, fileName, fileName1)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/memoryPressureInjectPoint", "return(0)")
	require.Nil(t, err)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/memoryUsageInjectPoint", "return(0)")
	require.Nil(t, err)

	backEnd2, err := backEndPool.alloc(ctx)
	require.Nil(t, err)
	require.IsType(t, &memoryBackEnd{}, backEnd2)

	err = backEndPool.dealloc(backEnd)
	require.Nil(t, err)

	err = backEndPool.dealloc(backEnd1)
	require.Nil(t, err)

	err = backEndPool.dealloc(backEnd2)
	require.Nil(t, err)

	time.Sleep(backgroundJobInterval * 3 / 2)

	_, err = os.Stat(fileName)
	require.True(t, os.IsNotExist(err))

	_, err = os.Stat(fileName1)
	require.True(t, os.IsNotExist(err))
}

// TestDirectoryBadPermission verifies that no permission to ls the directory does not prevent using it
// as a temporary file directory.
func TestDirectoryBadPermission(t *testing.T) {
	dataDir := t.TempDir()
	sortDir := filepath.Join(dataDir, config.DefaultSortDir)
	err := os.MkdirAll(sortDir, 0o755)
	require.Nil(t, err)

	err = os.Chmod(sortDir, 0o311) // no permission to `ls`
	defer func() {
		err := os.Chmod(sortDir, 0o755)
		require.Nil(t, err)
	}()
	require.Nil(t, err)

	conf := config.GetGlobalServerConfig()
	conf.DataDir = dataDir
	conf.Sorter.SortDir = sortDir
	conf.Sorter.MaxMemoryPercentage = 0 // force using files

	backEndPool, err := newBackEndPool(sortDir)
	require.Nil(t, err)
	require.NotNil(t, backEndPool)
	defer backEndPool.terminate()

	backEnd, err := backEndPool.alloc(context.Background())
	require.Nil(t, err)
	defer backEnd.free() //nolint:errcheck

	fileName := backEnd.(*fileBackEnd).fileName
	_, err = os.Stat(fileName)
	require.Nil(t, err)

	err = backEndPool.dealloc(backEnd)
	require.Nil(t, err)
}

// TestCleanUpSelf verifies that the backendPool correctly cleans up files used by itself on exit.
func TestCleanUpSelf(t *testing.T) {
	dataDir := t.TempDir()
	err := os.Chmod(dataDir, 0o755)
	require.Nil(t, err)

	sorterDir := filepath.Join(dataDir, config.DefaultSortDir)
	err = os.MkdirAll(sorterDir, 0o755)
	require.Nil(t, err)

	conf := config.GetDefaultServerConfig()
	conf.DataDir = dataDir
	conf.Sorter.SortDir = sorterDir
	conf.Sorter.MaxMemoryPercentage = 90                       // 90%
	conf.Sorter.MaxMemoryConsumption = 16 * 1024 * 1024 * 1024 // 16G
	config.StoreGlobalServerConfig(conf)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/memoryPressureInjectPoint", "return(100)")
	require.Nil(t, err)
	defer failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/memoryPressureInjectPoint") //nolint:errcheck

	backEndPool, err := newBackEndPool(sorterDir)
	require.Nil(t, err)
	require.NotNil(t, backEndPool)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	var fileNames []string
	for i := 0; i < 20; i++ {
		backEnd, err := backEndPool.alloc(ctx)
		require.Nil(t, err)
		require.IsType(t, &fileBackEnd{}, backEnd)

		fileName := backEnd.(*fileBackEnd).fileName
		_, err = os.Stat(fileName)
		require.Nil(t, err)

		fileNames = append(fileNames, fileName)
	}

	prefix := backEndPool.filePrefix
	require.NotEqual(t, "", prefix)

	for j := 100; j < 120; j++ {
		fileName := prefix + strconv.Itoa(j) + ".tmp"
		f, err := os.Create(fileName)
		require.Nil(t, err)
		err = f.Close()
		require.Nil(t, err)

		fileNames = append(fileNames, fileName)
	}

	backEndPool.terminate()

	for _, fileName := range fileNames {
		_, err = os.Stat(fileName)
		require.True(t, os.IsNotExist(err))
	}
}

type mockOtherProcess struct {
	dir    string
	prefix string
	flock  *fsutil.FileLock
	files  []string
}

func newMockOtherProcess(t *testing.T, dir string, prefix string) *mockOtherProcess {
	prefixLockPath := fmt.Sprintf("%s/%s", dir, sortDirLockFileName)
	flock, err := fsutil.NewFileLock(prefixLockPath)
	require.Nil(t, err)

	err = flock.Lock()
	require.Nil(t, err)

	return &mockOtherProcess{
		dir:    dir,
		prefix: prefix,
		flock:  flock,
	}
}

func (p *mockOtherProcess) writeMockFiles(t *testing.T, num int) {
	for i := 0; i < num; i++ {
		fileName := fmt.Sprintf("%s%d", p.prefix, i)
		f, err := os.Create(fileName)
		require.Nil(t, err)
		_ = f.Close()
		p.files = append(p.files, fileName)
	}
}

func (p *mockOtherProcess) changeLockPermission(t *testing.T, mode os.FileMode) {
	prefixLockPath := fmt.Sprintf("%s/%s", p.dir, sortDirLockFileName)
	err := os.Chmod(prefixLockPath, mode)
	require.Nil(t, err)
}

func (p *mockOtherProcess) unlock(t *testing.T) {
	err := p.flock.Unlock()
	require.Nil(t, err)
}

func (p *mockOtherProcess) assertFilesExist(t *testing.T) {
	for _, file := range p.files {
		_, err := os.Stat(file)
		require.Nil(t, err)
	}
}

func (p *mockOtherProcess) assertFilesNotExist(t *testing.T) {
	for _, file := range p.files {
		_, err := os.Stat(file)
		require.True(t, os.IsNotExist(err))
	}
}

// TestCleanUpStaleBasic verifies that the backendPool correctly cleans up stale temporary files
// left by other CDC processes that have exited abnormally.
func TestCleanUpStaleBasic(t *testing.T) {
	dir := t.TempDir()
	prefix := dir + "/sort-1-"

	mockP := newMockOtherProcess(t, dir, prefix)
	mockP.writeMockFiles(t, 100)
	mockP.unlock(t)
	mockP.assertFilesExist(t)

	backEndPool, err := newBackEndPool(dir)
	require.Nil(t, err)
	require.NotNil(t, backEndPool)
	defer backEndPool.terminate()

	mockP.assertFilesNotExist(t)
}

// TestFileLockConflict tests that if two backEndPools were to use the same sort-dir,
// and error would be returned by one of them.
func TestFileLockConflict(t *testing.T) {
	dir := t.TempDir()

	backEndPool1, err := newBackEndPool(dir)
	require.Nil(t, err)
	require.NotNil(t, backEndPool1)
	defer backEndPool1.terminate()

	backEndPool2, err := newBackEndPool(dir)
	require.Regexp(t, ".*file lock conflict.*", err)
	require.Nil(t, backEndPool2)
}

// TestCleanUpStaleBasic verifies that the backendPool correctly cleans up stale temporary files
// left by other CDC processes that have exited abnormally.
func TestCleanUpStaleLockNoPermission(t *testing.T) {
	dir := t.TempDir()
	prefix := dir + "/sort-1-"

	mockP := newMockOtherProcess(t, dir, prefix)
	mockP.writeMockFiles(t, 100)
	// set a bad permission
	mockP.changeLockPermission(t, 0o000)

	backEndPool, err := newBackEndPool(dir)
	require.Regexp(t, ".*permission denied.*", err)
	require.Nil(t, backEndPool)

	mockP.assertFilesExist(t)
}

// TestGetMemoryPressureFailure verifies that the backendPool can handle gracefully failures that happen when
// getting the current system memory pressure. Such a failure is usually caused by a lack of file descriptor quota
// set by the operating system.
func TestGetMemoryPressureFailure(t *testing.T) {
	origin := memory.MemTotal
	defer func() {
		memory.MemTotal = origin
	}()
	memory.MemTotal = func() (uint64, error) { return 0, nil }

	dir := t.TempDir()
	backEndPool, err := newBackEndPool(dir)
	require.Nil(t, err)
	require.NotNil(t, backEndPool)
	defer backEndPool.terminate()

	after := time.After(time.Second * 20)
	tick := time.Tick(time.Millisecond * 100)
	for {
		select {
		case <-after:
			t.Fatal("TestGetMemoryPressureFailure timed out")
		case <-tick:
			if backEndPool.memoryPressure() == 100 {
				return
			}
		}
	}
}

func TestCheckDataDirSatisfied(t *testing.T) {
	dir := t.TempDir()
	conf := config.GetGlobalServerConfig()
	conf.DataDir = dir
	config.StoreGlobalServerConfig(conf)

	p := "github.com/pingcap/tiflow/cdc/sorter/unified/" +
		"InjectCheckDataDirSatisfied"
	require.Nil(t, failpoint.Enable(p, ""))
	err := checkDataDirSatisfied()
	require.Nil(t, err)
	require.Nil(t, failpoint.Disable(p))
}
