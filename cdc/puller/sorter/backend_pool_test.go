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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/filelock"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type backendPoolSuite struct{}

var _ = check.SerialSuites(&backendPoolSuite{})

func (s *backendPoolSuite) TestBasicFunction(c *check.C) {
	defer testleak.AfterTest(c)()

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	conf.Sorter.MaxMemoryPressure = 90                         // 90%
	conf.Sorter.MaxMemoryConsumption = 16 * 1024 * 1024 * 1024 // 16G
	config.StoreGlobalServerConfig(conf)

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryPressureInjectPoint", "return(100)")
	c.Assert(err, check.IsNil)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()

	backEndPool := newBackEndPool("/tmp/sorter", "")
	c.Assert(backEndPool, check.NotNil)
	defer backEndPool.terminate()

	backEnd, err := backEndPool.alloc(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(backEnd, check.FitsTypeOf, &fileBackEnd{})
	fileName := backEnd.(*fileBackEnd).fileName
	c.Assert(fileName, check.Not(check.Equals), "")

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryPressureInjectPoint", "return(0)")
	c.Assert(err, check.IsNil)
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryUsageInjectPoint", "return(34359738368)")
	c.Assert(err, check.IsNil)

	backEnd1, err := backEndPool.alloc(ctx)
	c.Assert(err, check.IsNil)
	c.Assert(backEnd1, check.FitsTypeOf, &fileBackEnd{})
	fileName1 := backEnd1.(*fileBackEnd).fileName
	c.Assert(fileName1, check.Not(check.Equals), "")
	c.Assert(fileName1, check.Not(check.Equals), fileName)

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryPressureInjectPoint", "return(0)")
	c.Assert(err, check.IsNil)
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryUsageInjectPoint", "return(0)")
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

// TestCleanUpSelf verifies that the backendPool correctly cleans up files used by itself on exit.
func (s *backendPoolSuite) TestDirectoryBadPermission(c *check.C) {
	defer testleak.AfterTest(c)()

	dir := c.MkDir()
	err := os.Chmod(dir, 0o311) // no permission to `ls`
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	conf.Sorter.MaxMemoryPressure = 0 // force using files

	backEndPool := newBackEndPool(dir, "")
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

	err := os.MkdirAll("/tmp/sorter", 0o755)
	c.Assert(err, check.IsNil)

	conf := config.GetDefaultServerConfig()
	conf.Sorter.MaxMemoryPressure = 90                         // 90%
	conf.Sorter.MaxMemoryConsumption = 16 * 1024 * 1024 * 1024 // 16G
	config.StoreGlobalServerConfig(conf)

	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/memoryPressureInjectPoint", "return(100)")
	c.Assert(err, check.IsNil)

	backEndPool := newBackEndPool("/tmp/sorter", "")
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
	metaLockPath := fmt.Sprintf("%s/cdc-meta-lock", dir)
	metaLock, err := filelock.NewSimpleFileLock(metaLockPath)
	c.Assert(err, check.IsNil)
	defer metaLock.Unlock() //nolint:errcheck

	prefixLockPath := fmt.Sprintf("%slock", prefix)
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
	prefixLockPath := fmt.Sprintf("%slock", p.prefix)
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

func (p *mockOtherProcess) randomlyDeleteFiles(c *check.C) {
	numFiles := len(p.files)
	perm := rand.Perm(numFiles)
	for i := 0; i < numFiles/2; i++ {
		file := p.files[perm[i]]
		err := os.Remove(file)
		c.Assert(err, check.IsNil)
	}
}

// TestCleanUpStaleBasic verifies that the backendPool correctly cleans up stale temporary files
// left by other CDC processes that have exited abnormally.
func (s *backendPoolSuite) TestCleanUpStaleBasic(c *check.C) {
	defer testleak.AfterTest(c)()

	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/metaLockDelayInjectPoint", "return(500)")
	c.Assert(err, check.IsNil)
	defer failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/metaLockDelayInjectPoint") //nolint:errcheck

	dir := c.MkDir()
	prefix := dir + "/test-"
	prefix1 := dir + "/test1-"

	mockP := newMockOtherProcess(c, dir, prefix)
	mockP.writeMockFiles(c, 100)
	mockP.unlock(c)
	mockP.assertFilesExist(c)

	mockP1 := newMockOtherProcess(c, dir, prefix1)
	mockP1.writeMockFiles(c, 100)
	mockP1.assertFilesExist(c)

	backEndPool := newBackEndPool(dir, "")
	c.Assert(backEndPool, check.NotNil)
	defer backEndPool.terminate()

	mockP.assertFilesNotExist(c)
	mockP1.assertFilesExist(c)
	mockP1.unlock(c)

	err = backEndPool.cleanUpStaleFiles()
	c.Assert(err, check.IsNil)

	mockP1.assertFilesNotExist(c)
}

// TestCleanUpStaleContention verifies that the backendPool correctly cleans up stale temporary files
// if there are two new CDC processes starting up simultaneously and both are trying to clean up some stale files.
func (s *backendPoolSuite) TestCleanUpStaleContention(c *check.C) {
	defer testleak.AfterTest(c)()

	dir := c.MkDir()

	var (
		exitedProcesses  []*mockOtherProcess
		runningProcesses []*mockOtherProcess
	)

	for i := 0; i < 20; i++ {
		prefix := fmt.Sprintf("%s/testa%d-", dir, i)
		mockP := newMockOtherProcess(c, dir, prefix)
		mockP.writeMockFiles(c, 100)
		mockP.unlock(c)
		exitedProcesses = append(exitedProcesses, mockP)
	}

	for i := 0; i < 20; i++ {
		prefix := fmt.Sprintf("%s/testb%d-", dir, i)
		mockP := newMockOtherProcess(c, dir, prefix)
		mockP.writeMockFiles(c, 100)
		runningProcesses = append(runningProcesses, mockP)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			backEndPool := newBackEndPool(dir, "")
			c.Assert(backEndPool, check.NotNil)
			defer backEndPool.terminate()
		}()
	}

	wg.Wait()

	for _, mockP := range exitedProcesses {
		mockP.assertFilesNotExist(c)
	}
	for _, mockP := range runningProcesses {
		mockP.assertFilesExist(c)
		mockP.unlock(c)
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			backEndPool := newBackEndPool(dir, "")
			c.Assert(backEndPool, check.NotNil)
			defer backEndPool.terminate()
		}()
	}

	wg.Wait()
	for _, mockP := range runningProcesses {
		mockP.assertFilesNotExist(c)
	}
}

// TestCleanUpStaleBadLockPermission verifies that the backendPool does not panic or exit unexpectedly when it encounters
// lock files that it does not have the access permission to.
func (s *backendPoolSuite) TestCleanUpStaleBadLockPermission(c *check.C) {
	defer testleak.AfterTest(c)()

	dir := c.MkDir()
	prefix := dir + "/test-"

	mockP := newMockOtherProcess(c, dir, prefix)
	mockP.writeMockFiles(c, 100)
	mockP.changeLockPermission(c, 0o555) // read only
	mockP.unlock(c)
	mockP.assertFilesExist(c)

	backEndPool := newBackEndPool(dir, "")
	c.Assert(backEndPool, check.NotNil)
	defer backEndPool.terminate()

	mockP.assertFilesExist(c)
	mockP.changeLockPermission(c, 0o755) // read-write

	err := backEndPool.cleanUpStaleFiles()
	c.Assert(err, check.IsNil)

	mockP.assertFilesNotExist(c)
}

// TestCleanUpStaleFileRemovedUnexpectedly verifies that the backendPool does not panic or exit unexpectedly when it races with
// a manual deletion of a stale file.
func (s *backendPoolSuite) TestCleanUpStaleFileRemovedUnexpectedly(c *check.C) {
	defer testleak.AfterTest(c)()

	dir := c.MkDir()
	prefix := dir + "/test-"

	mockP := newMockOtherProcess(c, dir, prefix)
	mockP.writeMockFiles(c, 100)
	mockP.unlock(c)
	mockP.assertFilesExist(c)

	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/deleteStaleFileDelayInjectPoint", "pause")
	defer failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/deleteStaleFileDelayInjectPoint") //nolint:errcheck

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		backEndPool := newBackEndPool(dir, "")
		c.Assert(backEndPool, check.NotNil)
		defer backEndPool.terminate()
	}()

	time.Sleep(3 * time.Second)
	mockP.randomlyDeleteFiles(c)
	_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/deleteStaleFileDelayInjectPoint") //nolint:errcheck
	c.Assert(err, check.IsNil)

	wg.Wait()

	mockP.assertFilesNotExist(c)
}
