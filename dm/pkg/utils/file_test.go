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

package utils

import (
	"os"
	"path/filepath"

	. "github.com/pingcap/check"

	"github.com/pingcap/dm/pkg/terror"
)

var _ = Suite(&testFileSuite{})

type testFileSuite struct{}

func (t *testFileSuite) TestFile(c *C) {
	// dir not exists
	c.Assert(IsFileExists("invalid-path"), IsFalse)
	c.Assert(IsDirExists("invalid-path"), IsFalse)
	size, err := GetFileSize("invalid-path")
	c.Assert(terror.ErrGetFileSize.Equal(err), IsTrue)
	c.Assert(size, Equals, int64(0))

	// dir exists
	d := c.MkDir()
	c.Assert(IsFileExists(d), IsFalse)
	c.Assert(IsDirExists(d), IsTrue)
	size, err = GetFileSize(d)
	c.Assert(terror.ErrGetFileSize.Equal(err), IsTrue)
	c.Assert(size, Equals, int64(0))

	// file not exists
	f := filepath.Join(d, "text-file")
	c.Assert(IsFileExists(f), IsFalse)
	c.Assert(IsDirExists(f), IsFalse)
	size, err = GetFileSize(f)
	c.Assert(terror.ErrGetFileSize.Equal(err), IsTrue)
	c.Assert(size, Equals, int64(0))

	// create a file
	c.Assert(os.WriteFile(f, []byte("some content"), 0o644), IsNil)
	c.Assert(IsFileExists(f), IsTrue)
	c.Assert(IsDirExists(f), IsFalse)
	size, err = GetFileSize(f)
	c.Assert(err, IsNil)
	c.Assert(size, Equals, int64(len("some content")))
}
