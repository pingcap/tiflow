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

package sorter

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filelock"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type backendPoolSuite struct{}

var _ = check.SerialSuites(&backendPoolSuite{})

func (s *backendPoolSuite) TestBasicFunction(c *check.C) {
	defer testleak.AfterTest(c)()

	dataDir := c.MkDir()
	err := os.MkdirAll(dataDir, 0o755)
	c.Assert(err, check.IsNil)

	sortDir := filepath.Join(dataDir, config.DefaultSortDir)
	err = os.MkdirAll(sortDir, 0o755)
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	conf.DataDir = dataDir
	conf.Sorter.SortDir = sortDir
	conf.Sorter.MaxMemoryPressure = 90                         // 90%
	conf.Sorter.MaxMemoryConsumption = 16 * 1024 * 1024 * 1024 // 16G
	config.StoreGlobalServerConfig(conf)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/puller/sorter/memoryPressureInjectPoint", "return(100)")
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	backEndPool, err := newBackEndPool(sortDir, "")
	c.Assert(err, check.IsNil)
	c.Assert(backEndPool, check.NotNil)
	defer backEndPool.terminate()

	backEnd, err := backEndPool.alloc(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(backEnd, check.FitsTypeOf, &fileBackEnd{})
	fileName := backEnd.(*fileBackEnd).fileName
	c.Assert(fileName, check.Not(check.Equals), "")

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/puller/sorter/memoryPressureInjectPoint", "return(0)")
	c.Assert(err, check.IsNil)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/puller/sorter/memoryUsageInjectPoint", "return(34359738368)")
	c.Assert(err, check.IsNil)

	backEnd1, err := backEndPool.alloc(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(backEnd1, check.FitsTypeOf, &fileBackEnd{})
	fileName1 := backEnd1.(*fileBackEnd).fileName
	c.Assert(fileName1, check.Not(check.Equals), "")
	c.Assert(fileName1, check.Not(check.Equals), fileName)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/puller/sorter/memoryPressureInjectPoint", "return(0)")
	c.Assert(err, check.IsNil)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/puller/sorter/memoryUsageInjectPoint", "return(0)")
	c.Assert(err, check.IsNil)

	backEnd2, err := backEndPool.alloc(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(backEnd2, check.FitsTypeOf, &memoryBackEnd{})

	err = backEndPool.dealloc(backEnd)
	c.Assert(err, check.IsNil)

	err = backEndPool.dealloc(backEnd1)
	c.Assert(err, check.IsNil)

	err = backEndPool.dealloc(backEnd2)
	c.Assert(err, check.IsNil)

	time.Sleep(backgroundJobInterval * 3 / 2)

	_, err = os.Stat(fileName)
	c.Assert(os.IsNotExist(err), check.IsTrue)

	_, err = os.Stat(fileName1)
	c.Assert(os.IsNotExist(err), check.IsTrue)
}

// TestDirectoryBadPermission verifies that no permission to ls the directory does not prevent using it
// as a temporary file directory.
func (s *backendPoolSuite) TestDirectoryBadPermission(c *check.C) {
	defer testleak.AfterTest(c)()

	dataDir := c.MkDir()
	sortDir := filepath.Join(dataDir, config.DefaultSortDir)
	err := os.MkdirAll(sortDir, 0o755)
	c.Assert(err, check.IsNil)

	err = os.Chmod(sortDir, 0o311) // no permission to `ls`
	c.Assert(err, check.IsNil)

	conf := config.GetGlobalServerConfig()
	conf.DataDir = dataDir
	conf.Sorter.SortDir = sortDir
	conf.Sorter.MaxMemoryPressure = 0 // force using files

	backEndPool, err := newBackEndPool(sortDir, "")
	c.Assert(err, check.IsNil)
	c.Assert(backEndPool, check.NotNil)
	defer backEndPool.terminate()

	backEnd, err := backEndPool.alloc(context.Background())
	c.Assert(err, check.IsNil)
	defer backEnd.free() //nolint:errcheck

	fileName := backEnd.(*fileBackEnd).fileName
	_, err = os.Stat(fileName)
	c.Assert(err, check.IsNil) // assert that the file exists

	err = backEndPool.dealloc(backEnd)
	c.Assert(err, check.IsNil)
}

// TestCleanUpSelf verifies that the backendPool correctly cleans up files used by itself on exit.
func (s *backendPoolSuite) TestCleanUpSelf(c *check.C) {
	defer testleak.AfterTest(c)()

	dataDir := c.MkDir()
	err := os.Chmod(dataDir, 0o755)
	c.Assert(err, check.IsNil)

	sorterDir := filepath.Join(dataDir, config.DefaultSortDir)
	err = os.MkdirAll(sorterDir, 0o755)
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	conf.DataDir = dataDir
	conf.Sorter.SortDir = sorterDir
	conf.Sorter.MaxMemoryPressure = 90                         // 90%
	conf.Sorter.MaxMemoryConsumption = 16 * 1024 * 1024 * 1024 // 16G
	config.StoreGlobalServerConfig(conf)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/puller/sorter/memoryPressureInjectPoint", "return(100)")
	c.Assert(err, check.IsNil)
	defer failpoint.Disable("github.com/pingcap/tiflow/cdc/puller/sorter/memoryPressureInjectPoint") //nolint:errcheck

	backEndPool, err := newBackEndPool(sorterDir, "")
	c.Assert(err, check.IsNil)
	c.Assert(backEndPool, check.NotNil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	var fileNames []string
	for i := 0; i < 20; i++ {
		backEnd, err := backEndPool.alloc(ctx)
		c.Assert(err, check.IsNil)
		c.Assert(backEnd, check.FitsTypeOf, &fileBackEnd{})

		fileName := backEnd.(*fileBackEnd).fileName
		_, err = os.Stat(fileName)
		c.Assert(err, check.IsNil)

		fileNames = append(fileNames, fileName)
	}

	prefix := backEndPool.filePrefix
	c.Assert(prefix, check.Not(check.Equals), "")

	for j := 100; j < 120; j++ {
		fileName := prefix + strconv.Itoa(j) + ".tmp"
		f, err := os.Create(fileName)
		c.Assert(err, check.IsNil)
		err = f.Close()
		c.Assert(err, check.IsNil)

		fileNames = append(fileNames, fileName)
	}

	backEndPool.terminate()

	for _, fileName := range fileNames {
		_, err = os.Stat(fileName)
		c.Assert(os.IsNotExist(err), check.IsTrue)
	}
}

type mockOtherProcess struct {
	dir    string
	prefix string
	flock  *filelock.FileLock
	files  []string
}

func newMockOtherProcess(c *check.C, dir string, prefix string) *mockOtherProcess {
	prefixLockPath := fmt.Sprintf("%s/%s", dir, sortDirLockFileName)
	flock, err := filelock.NewFileLock(prefixLockPath)
	c.Assert(err, check.IsNil)

	err = flock.Lock()
	c.Assert(err, check.IsNil)

	return &mockOtherProcess{
		dir:    dir,
		prefix: prefix,
		flock:  flock,
	}
}

func (p *mockOtherProcess) writeMockFiles(c *check.C, num int) {
	for i := 0; i < num; i++ {
		fileName := fmt.Sprintf("%s%d", p.prefix, i)
		f, err := os.Create(fileName)
		c.Assert(err, check.IsNil)
		_ = f.Close()
		p.files = append(p.files, fileName)
	}
}

func (p *mockOtherProcess) changeLockPermission(c *check.C, mode os.FileMode) {
	prefixLockPath := fmt.Sprintf("%s/%s", p.dir, sortDirLockFileName)
	err := os.Chmod(prefixLockPath, mode)
	c.Assert(err, check.IsNil)
}

func (p *mockOtherProcess) unlock(c *check.C) {
	err := p.flock.Unlock()
	c.Assert(err, check.IsNil)
}

func (p *mockOtherProcess) assertFilesExist(c *check.C) {
	for _, file := range p.files {
		_, err := os.Stat(file)
		c.Assert(err, check.IsNil)
	}
}

func (p *mockOtherProcess) assertFilesNotExist(c *check.C) {
	for _, file := range p.files {
		_, err := os.Stat(file)
		c.Assert(os.IsNotExist(err), check.IsTrue)
	}
}

// TestCleanUpStaleBasic verifies that the backendPool correctly cleans up stale temporary files
// left by other CDC processes that have exited abnormally.
func (s *backendPoolSuite) TestCleanUpStaleBasic(c *check.C) {
	defer testleak.AfterTest(c)()

	dir := c.MkDir()
	prefix := dir + "/sort-1-"

	mockP := newMockOtherProcess(c, dir, prefix)
	mockP.writeMockFiles(c, 100)
	mockP.unlock(c)
	mockP.assertFilesExist(c)

	backEndPool, err := newBackEndPool(dir, "")
	c.Assert(err, check.IsNil)
	c.Assert(backEndPool, check.NotNil)
	defer backEndPool.terminate()

	mockP.assertFilesNotExist(c)
}

// TestFileLockConflict tests that if two backEndPools were to use the same sort-dir,
// and error would be returned by one of them.
func (s *backendPoolSuite) TestFileLockConflict(c *check.C) {
	defer testleak.AfterTest(c)()
	dir := c.MkDir()

	backEndPool1, err := newBackEndPool(dir, "")
	c.Assert(err, check.IsNil)
	c.Assert(backEndPool1, check.NotNil)
	defer backEndPool1.terminate()

	backEndPool2, err := newBackEndPool(dir, "")
	c.Assert(err, check.ErrorMatches, ".*file lock conflict.*")
	c.Assert(backEndPool2, check.IsNil)
}

// TestCleanUpStaleBasic verifies that the backendPool correctly cleans up stale temporary files
// left by other CDC processes that have exited abnormally.
func (s *backendPoolSuite) TestCleanUpStaleLockNoPermission(c *check.C) {
	defer testleak.AfterTest(c)()

	dir := c.MkDir()
	prefix := dir + "/sort-1-"

	mockP := newMockOtherProcess(c, dir, prefix)
	mockP.writeMockFiles(c, 100)
	// set a bad permission
	mockP.changeLockPermission(c, 0o000)

	backEndPool, err := newBackEndPool(dir, "")
	c.Assert(err, check.ErrorMatches, ".*permission denied.*")
	c.Assert(backEndPool, check.IsNil)

	mockP.assertFilesExist(c)
}

// TestGetMemoryPressureFailure verifies that the backendPool can handle gracefully failures that happen when
// getting the current system memory pressure. Such a failure is usually caused by a lack of file descriptor quota
// set by the operating system.
func (s *backendPoolSuite) TestGetMemoryPressureFailure(c *check.C) {
	defer testleak.AfterTest(c)()

	origin := memory.MemTotal
	defer func() {
		memory.MemTotal = origin
	}()
	memory.MemTotal = func() (uint64, error) { return 0, nil }

	dir := c.MkDir()
	backEndPool, err := newBackEndPool(dir, "")
	c.Assert(err, check.IsNil)
	c.Assert(backEndPool, check.NotNil)
	defer backEndPool.terminate()

	after := time.After(time.Second * 20)
	tick := time.Tick(time.Millisecond * 100)
	for {
		select {
		case <-after:
			c.Fatal("TestGetMemoryPressureFailure timed out")
		case <-tick:
			if backEndPool.memoryPressure() == 100 {
				return
			}
		}
	}
}
