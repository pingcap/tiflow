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

package util

import (
	"io/ioutil"
	"os"
	"os/user"
	"path/filepath"
	"runtime"

	"github.com/pingcap/check"
)

type fileUtilSuite struct{}

var _ = check.Suite(&fileUtilSuite{})

func (s *fileUtilSuite) TestIsDirWritable(c *check.C) {
	dir := c.MkDir()
	err := IsDirWritable(dir)
	c.Assert(err, check.IsNil)

	err = os.Chmod(dir, 0400)
	c.Assert(err, check.IsNil)
	me, err := user.Current()
	c.Assert(err, check.IsNil)
	if me.Name == "root" || runtime.GOOS == "windows" {
		// chmod is not supported under windows.
		c.Skip("test case is running as a superuser or in windows")
	}
	err = IsDirWritable(dir)
	c.Assert(err, check.ErrorMatches, ".*permission denied")
}

func (s *fileUtilSuite) TestIsDirAndWritable(c *check.C) {
	dir := c.MkDir()
	path := filepath.Join(dir, "file.test")

	err := IsDirAndWritable(path)
	c.Assert(err, check.ErrorMatches, ".*no such file or directory")

	err = ioutil.WriteFile(path, nil, 0600)
	c.Assert(err, check.IsNil)
	err = IsDirAndWritable(path)
	c.Assert(err, check.ErrorMatches, ".*is not a directory")

	err = IsDirAndWritable(dir)
	c.Assert(err, check.IsNil)
}
