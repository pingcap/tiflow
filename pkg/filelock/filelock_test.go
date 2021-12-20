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
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type fileLockSuite struct{}

var _ = check.SerialSuites(&fileLockSuite{})

func Test(t *testing.T) { check.TestingT(t) }

// TestBasicCase tests the basic scenarios of using FileLock and all operations are expected to be successful
func (s *fileLockSuite) TestBasicCase(c *check.C) {
	defer testleak.AfterTest(c)()

	path := c.MkDir() + "/test_lock"

	fileLock, err := NewFileLock(path)
	c.Assert(err, check.IsNil)
	defer fileLock.Close() //nolint:errcheck

	fileLockCheck, err := NewFileLock(path)
	c.Assert(err, check.IsNil)
	defer fileLockCheck.Close() //nolint:errcheck

	isLocked, err := fileLock.IsLocked()
	c.Assert(err, check.IsNil)
	c.Assert(isLocked, check.IsFalse)

	err = fileLock.Lock()
	c.Assert(err, check.IsNil)

	isLocked, err = fileLockCheck.IsLocked()
	c.Assert(err, check.IsNil)
	c.Assert(isLocked, check.IsTrue)

	err = fileLock.Unlock()
	c.Assert(err, check.IsNil)

	isLocked, err = fileLockCheck.IsLocked()
	c.Assert(err, check.IsNil)
	c.Assert(isLocked, check.IsFalse)

	isLocked, err = fileLock.IsLocked()
	c.Assert(err, check.IsNil)
	c.Assert(isLocked, check.IsFalse)
}

// TestBadPath tests the case where the file path is not accessible
func (s *fileLockSuite) TestBadPath(c *check.C) {
	defer testleak.AfterTest(c)()

	path := c.MkDir() + "/bad_path/bad_file"

	_, err := NewFileLock(path)
	c.Assert(err, check.ErrorMatches, ".*no such file.*")
}

// TestBadPath tests the case where the file is locked twice
// We do not expect this to happen in TiCDC.
func (s *fileLockSuite) TestDuplicateLocking(c *check.C) {
	defer testleak.AfterTest(c)()

	path := c.MkDir() + "/test_lock"

	fileLock, err := NewFileLock(path)
	c.Assert(err, check.IsNil)
	defer fileLock.Close() //nolint:errcheck

	fileLock2, err := NewFileLock(path)
	c.Assert(err, check.IsNil)
	defer fileLock2.Close() //nolint:errcheck

	err = fileLock.Lock()
	c.Assert(err, check.IsNil)

	err = fileLock2.Lock()
	c.Assert(err, check.ErrorMatches, ".*ErrConflictingFileLocks.*")
}
