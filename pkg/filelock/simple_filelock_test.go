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

package filelock

import (
	"os"
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
)

type simpleFileLockSuite struct{}

var _ = check.SerialSuites(&simpleFileLockSuite{})

// TestBasic tests the basic usage of SimpleFileLock
func (s *simpleFileLockSuite) TestBasic(c *check.C) {
	defer testleak.AfterTest(c)()

	path := c.MkDir() + "/test_simple_lock"
	fileLock, err := NewSimpleFileLock(path)
	c.Assert(err, check.IsNil)

	err = fileLock.Unlock()
	c.Assert(err, check.IsNil)
}

// TestConflictNormal tests the situation where two processes contends for a SimpleFileLock
func (s *simpleFileLockSuite) TestConflictNormal(c *check.C) {
	defer testleak.AfterTest(c)()

	path := c.MkDir() + "/test_simple_lock"
	fileLock, err := NewSimpleFileLock(path)
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		startTime := time.Now()

		fileLock1, err := NewSimpleFileLock(path)

		c.Assert(err, check.IsNil)
		timeTaken := time.Since(startTime)
		c.Assert(timeTaken, check.LessEqual, 8*time.Second)
		c.Assert(timeTaken, check.GreaterEqual, 4*time.Second)

		err = fileLock1.Unlock()
		c.Check(err, check.IsNil)
	}()

	time.Sleep(5 * time.Second)
	err = fileLock.Unlock()
	c.Assert(err, check.IsNil)

	wg.Wait()

	_, err = os.Stat(path)
	c.Assert(os.IsNotExist(err), check.IsTrue)
}

// TestConflictStale tests the situation where a stale lock is left by a terminated process
func (s *simpleFileLockSuite) TestConflictStale(c *check.C) {
	defer testleak.AfterTest(c)()

	path := c.MkDir() + "/test_simple_lock"
	_, err := NewSimpleFileLock(path)
	c.Assert(err, check.IsNil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		startTime := time.Now()

		fileLock1, err := NewSimpleFileLock(path)

		c.Assert(err, check.IsNil)
		timeTaken := time.Since(startTime)
		c.Assert(timeTaken, check.LessEqual, 25*time.Second)
		c.Assert(timeTaken, check.GreaterEqual, 9*time.Second)

		err = fileLock1.Unlock()
		c.Check(err, check.IsNil)
	}()

	wg.Wait()

	_, err = os.Stat(path)
	c.Assert(os.IsNotExist(err), check.IsTrue)
}
