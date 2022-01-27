// Copyright 2019 PingCAP, Inc.
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

package relay

import (
	"os"
	"path/filepath"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
)

var _ = Suite(&testFileSuite{})

type testFileSuite struct{}

func (t *testFileSuite) TestCollectBinlogFiles(c *C) {
	var (
		valid = []string{
			"mysql-bin.000001",
			"mysql-bin.000002",
			"mysql-bin.000003",
			"mysql-bin.000004",
		}
		invalid = []string{
			"mysql-bin.invalid01",
			"mysql-bin.invalid02",
		}
		meta = []string{
			utils.MetaFilename,
			utils.MetaFilename + ".tmp",
		}
	)

	files, err := CollectAllBinlogFiles("")
	c.Assert(err, NotNil)
	c.Assert(files, IsNil)

	dir := c.MkDir()

	// create all valid binlog files
	for _, fn := range valid {
		err = os.WriteFile(filepath.Join(dir, fn), nil, 0o600)
		c.Assert(err, IsNil)
	}
	files, err = CollectAllBinlogFiles(dir)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid)

	// create some invalid binlog files
	for _, fn := range invalid {
		err = os.WriteFile(filepath.Join(dir, fn), nil, 0o600)
		c.Assert(err, IsNil)
	}
	files, err = CollectAllBinlogFiles(dir)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid)

	// create some invalid meta files
	for _, fn := range meta {
		err = os.WriteFile(filepath.Join(dir, fn), nil, 0o600)
		c.Assert(err, IsNil)
	}
	files, err = CollectAllBinlogFiles(dir)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid)

	// collect newer files, none
	files, err = CollectBinlogFilesCmp(dir, valid[len(valid)-1], FileCmpBigger)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, []string{})

	// collect newer files, some
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpBigger)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid[1:])

	// collect newer or equal files, all
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpBiggerEqual)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid)

	// collect newer or equal files, some
	files, err = CollectBinlogFilesCmp(dir, valid[1], FileCmpBiggerEqual)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid[1:])

	// collect older files, none
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpLess)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, []string{})

	// collect older files, some
	files, err = CollectBinlogFilesCmp(dir, valid[len(valid)-1], FileCmpLess)
	c.Assert(err, IsNil)
	c.Assert(files, DeepEquals, valid[:len(valid)-1])
}

func (t *testFileSuite) TestCollectBinlogFilesCmp(c *C) {
	var (
		dir         string
		baseFile    string
		cmp         = FileCmpEqual
		binlogFiles = []string{
			"mysql-bin.000001",
			"mysql-bin.000002",
			"mysql-bin.000003",
			"mysql-bin.000004",
		}
	)

	// empty dir
	files, err := CollectBinlogFilesCmp(dir, baseFile, cmp)
	c.Assert(terror.ErrEmptyRelayDir.Equal(err), IsTrue)
	c.Assert(files, IsNil)

	// empty base filename, not found
	dir = c.MkDir()
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	c.Assert(errors.IsNotFound(err), IsTrue)
	c.Assert(files, IsNil)

	// base file not found
	baseFile = utils.MetaFilename
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	c.Assert(errors.IsNotFound(err), IsTrue)
	c.Assert(files, IsNil)

	// create a meta file
	filename := filepath.Join(dir, utils.MetaFilename)
	err = os.WriteFile(filename, nil, 0o600)
	c.Assert(err, IsNil)

	// invalid base filename, is a meta filename
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	c.Assert(err, ErrorMatches, ".*invalid binlog filename.*")
	c.Assert(files, IsNil)

	// create some binlog files
	for _, f := range binlogFiles {
		filename = filepath.Join(dir, f)
		err = os.WriteFile(filename, nil, 0o600)
		c.Assert(err, IsNil)
	}

	// > base file
	cmp = FileCmpBigger
	var i int
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, IsNil)
		c.Assert(files, DeepEquals, binlogFiles[i+1:])
	}

	// >= base file
	cmp = FileCmpBiggerEqual
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, IsNil)
		c.Assert(files, DeepEquals, binlogFiles[i:])
	}

	// < base file
	cmp = FileCmpLess
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, IsNil)
		c.Assert(files, DeepEquals, binlogFiles[:i])
	}

	// add a basename mismatch binlog file
	filename = filepath.Join(dir, "bin-mysql.100000")
	err = os.WriteFile(filename, nil, 0o600)
	c.Assert(err, IsNil)

	// test again, should ignore it
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, IsNil)
		c.Assert(files, DeepEquals, binlogFiles[:i])
	}

	// other cmp not supported yet
	cmps := []FileCmp{FileCmpLessEqual, FileCmpEqual}
	for _, cmp = range cmps {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		c.Assert(err, ErrorMatches, ".*not supported.*")
		c.Assert(files, IsNil)
	}
}

func (t *testFileSuite) TestGetFirstBinlogName(c *C) {
	var (
		baseDir = c.MkDir()
		uuid    = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		subDir  = filepath.Join(baseDir, uuid)
	)

	// sub directory not exist
	name, err := getFirstBinlogName(baseDir, uuid)
	c.Assert(err, ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")
	c.Assert(name, Equals, "")

	// empty directory
	err = os.MkdirAll(subDir, 0o700)
	c.Assert(err, IsNil)
	name, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, ErrorMatches, ".*not found.*")
	c.Assert(name, Equals, "")

	// has file, but not a valid binlog file. Now the error message is binlog files not found
	filename := "invalid.bin"
	err = os.WriteFile(filepath.Join(subDir, filename), nil, 0o600)
	c.Assert(err, IsNil)
	_, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, ErrorMatches, ".*not found.*")
	err = os.Remove(filepath.Join(subDir, filename))
	c.Assert(err, IsNil)

	// has a valid binlog file
	filename = "z-mysql-bin.000002" // z prefix, make it become not the _first_ if possible.
	err = os.WriteFile(filepath.Join(subDir, filename), nil, 0o600)
	c.Assert(err, IsNil)
	name, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, filename)

	// has one more earlier binlog file
	filename = "z-mysql-bin.000001"
	err = os.WriteFile(filepath.Join(subDir, filename), nil, 0o600)
	c.Assert(err, IsNil)
	name, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, filename)

	// has a meta file
	err = os.WriteFile(filepath.Join(subDir, utils.MetaFilename), nil, 0o600)
	c.Assert(err, IsNil)
	name, err = getFirstBinlogName(baseDir, uuid)
	c.Assert(err, IsNil)
	c.Assert(name, Equals, filename)
}

func (t *testFileSuite) TestFileSizeUpdated(c *C) {
	var (
		filename   = "mysql-bin.000001"
		filePath   = filepath.Join(c.MkDir(), filename)
		data       = []byte("meaningless file content")
		latestSize = int64(len(data))
	)

	// file not exists
	cmp, err := fileSizeUpdated(filePath, latestSize)
	c.Assert(err, ErrorMatches, ".*(no such file or directory|The system cannot find the file specified).*")
	c.Assert(cmp, Equals, 0)

	// create and write the file
	err = os.WriteFile(filePath, data, 0o600)
	c.Assert(err, IsNil)

	// equal
	cmp, err = fileSizeUpdated(filePath, latestSize)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 0)

	// less than
	cmp, err = fileSizeUpdated(filePath, latestSize+1)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, -1)

	// greater than
	cmp, err = fileSizeUpdated(filePath, latestSize-1)
	c.Assert(err, IsNil)
	c.Assert(cmp, Equals, 1)
}
