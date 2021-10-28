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

package binlog

import (
	. "github.com/pingcap/check"
)

var _ = Suite(&testFilenameSuite{})

type testFilenameSuite struct{}

func (t *testFilenameSuite) TestFilenameCmp(c *C) {
	f1 := Filename{
		BaseName: "mysql-bin",
		Seq:      "000001",
		SeqInt64: 1,
	}
	f2 := Filename{
		BaseName: "mysql-bin",
		Seq:      "000002",
		SeqInt64: 2,
	}
	f3 := Filename{
		BaseName: "mysql-bin",
		Seq:      "000001", // == f1
		SeqInt64: 1,
	}
	f4 := Filename{
		BaseName: "bin-mysq", // diff BaseName
		Seq:      "000001",
		SeqInt64: 1,
	}

	c.Assert(f1.LessThan(f2), IsTrue)
	c.Assert(f1.GreaterThanOrEqualTo(f2), IsFalse)
	c.Assert(f1.GreaterThan(f2), IsFalse)

	c.Assert(f2.LessThan(f1), IsFalse)
	c.Assert(f2.GreaterThanOrEqualTo(f1), IsTrue)
	c.Assert(f2.GreaterThan(f1), IsTrue)

	c.Assert(f1.LessThan(f3), IsFalse)
	c.Assert(f1.GreaterThanOrEqualTo(f3), IsTrue)
	c.Assert(f1.GreaterThan(f3), IsFalse)

	c.Assert(f1.LessThan(f4), IsFalse)
	c.Assert(f1.GreaterThanOrEqualTo(f4), IsFalse)
	c.Assert(f1.GreaterThan(f4), IsFalse)
}

func (t *testFilenameSuite) TestParseFilenameAndGetFilenameIndex(c *C) {
	cases := []struct {
		filenameStr string
		filename    Filename
		index       int64
		errMsgReg   string
	}{
		{
			// valid
			filenameStr: "mysql-bin.666666",
			filename:    Filename{"mysql-bin", "666666", 666666},
			index:       666666,
		},
		{
			// valid
			filenameStr: "mysql-bin.000888",
			filename:    Filename{"mysql-bin", "000888", 888},
			index:       888,
		},
		{
			// empty filename
			filenameStr: "",
			errMsgReg:   ".*invalid binlog filename.*",
		},
		{
			// negative seq number
			filenameStr: "mysql-bin.-666666",
			errMsgReg:   ".*invalid binlog filename.*",
		},
		{
			// zero seq number
			filenameStr: "mysql-bin.000000",
			errMsgReg:   ".*invalid binlog filename.*",
		},
		{
			// too many separators
			filenameStr: "mysql.bin.666666",
			errMsgReg:   ".*invalid binlog filename.*",
		},
		{
			// too less separators
			filenameStr: "mysql-bin",
			errMsgReg:   ".*invalid binlog filename.*",
		},
		{
			// invalid seq number
			filenameStr: "mysql-bin.666abc",
			errMsgReg:   ".*invalid binlog filename.*",
		},
		{
			// invalid seq number
			filenameStr: "mysql-bin.def666",
			errMsgReg:   ".*invalid binlog filename.*",
		},
		{
			// invalid seq number
			filenameStr: "mysql.bin",
			errMsgReg:   ".*invalid binlog filename.*",
		},
	}

	for _, cs := range cases {
		f, err := ParseFilename(cs.filenameStr)
		if len(cs.errMsgReg) > 0 {
			c.Assert(err, ErrorMatches, cs.errMsgReg)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(f, DeepEquals, cs.filename)

		idx, err := GetFilenameIndex(cs.filenameStr)
		if len(cs.errMsgReg) > 0 {
			c.Assert(err, ErrorMatches, cs.errMsgReg)
		} else {
			c.Assert(err, IsNil)
		}
		c.Assert(idx, Equals, cs.index)
	}
}

func (t *testFilenameSuite) TestVerifyFilename(c *C) {
	cases := []struct {
		filename string
		valid    bool
	}{
		{
			// valid
			filename: "mysql-bin.666666",
			valid:    true,
		},
		{
			// empty filename
			filename: "",
		},
		{
			// negative seq number
			filename: "mysql-bin.-666666",
		},
		{
			// zero seq number
			filename: "mysql-bin.000000",
		},
		{
			// too many separators
			filename: "mysql.bin.666666",
		},
		{
			// too less separators
			filename: "mysql-bin",
		},
		{
			// invalid seq number
			filename: "mysql-bin.666abc",
		},
		{
			// invalid seq number
			filename: "mysql-bin.def666",
		},
	}

	for _, cs := range cases {
		c.Assert(VerifyFilename(cs.filename), Equals, cs.valid)
	}
}

func (t *testFilenameSuite) TestConstructFilename(c *C) {
	cases := []struct {
		baseName string
		seq      string
		filename string
	}{
		{
			baseName: "mysql-bin",
			seq:      "000666",
			filename: "mysql-bin.000666",
		},
	}

	for _, cs := range cases {
		c.Assert(ConstructFilename(cs.baseName, cs.seq), Equals, cs.filename)
	}
}

func (t *testFilenameSuite) TestConstructFilenameWithUUIDSuffix(c *C) {
	cases := []struct {
		originalName   Filename
		suffix         string
		withSuffixName string
	}{
		{
			originalName:   Filename{"mysql-bin", "000001", 1},
			suffix:         "666666",
			withSuffixName: "mysql-bin|666666.000001",
		},
	}

	for _, cs := range cases {
		c.Assert(ConstructFilenameWithUUIDSuffix(cs.originalName, cs.suffix), Equals, cs.withSuffixName)
		baseName, uuidSuffix, seq, err := SplitFilenameWithUUIDSuffix(cs.withSuffixName)
		c.Assert(err, IsNil)
		c.Assert(baseName, Equals, cs.originalName.BaseName)
		c.Assert(uuidSuffix, Equals, cs.suffix)
		c.Assert(seq, Equals, cs.originalName.Seq)
	}

	invalidFileName := []string{
		"mysql-bin.000001",
		"mysql-bin.000001.000001",
		"mysql-bin|000001",
		"mysql-bin|000001|000001",
		"mysql-bin|000001.000002.000003",
	}

	for _, fileName := range invalidFileName {
		_, _, _, err := SplitFilenameWithUUIDSuffix(fileName)
		c.Assert(err, ErrorMatches, ".*invalid binlog filename with uuid suffix.*")
	}
}
