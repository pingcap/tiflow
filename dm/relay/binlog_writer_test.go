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
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pingcap/check"

	"github.com/pingcap/ticdc/dm/pkg/log"
)

func TestBinlogWriterSuite(t *testing.T) {
	var _ = Suite(&testBinlogWriterSuite{})
	TestingT(t)
}

type testBinlogWriterSuite struct{}

func (t *testBinlogWriterSuite) TestWrite(c *C) {
	dir := c.MkDir()
	filename := filepath.Join(dir, "test-mysql-bin.000001")
	var (
		allData bytes.Buffer
		data1   = []byte("test-data")
	)

	w := NewBinlogWriter(log.L())
	c.Assert(w, NotNil)

	// check status, stageNew
	fwStatus := w.Status()
	c.Assert(fwStatus.Filename, Equals, filename)
	c.Assert(fwStatus.Offset, Equals, int64(allData.Len()))
	fwStatusStr := fwStatus.String()
	c.Assert(strings.Contains(fwStatusStr, "filename"), IsTrue)

	// not opened
	err := w.Write(data1)
	c.Assert(err, ErrorMatches, "not opened")

	// open non exist dir
	err = w.Open(filepath.Join(dir, "not-exist", "bin.000001"))
	c.Assert(err, ErrorMatches, "not exist")

	{
		// normal call flow
		err = w.Open(filename)
		c.Assert(err, IsNil)
		c.Assert(w.file, NotNil)
		c.Assert(w.filename, Equals, filename)
		c.Assert(w.offset.Load(), Equals, 0)

		err = w.Write(data1)
		c.Assert(err, IsNil)
		allData.Write(data1)

		fwStatus = w.Status()
		c.Assert(fwStatus.Filename, Equals, filename)
		c.Assert(fwStatus.Offset, Equals, len(data1))

		// write data again
		data2 := []byte("another-data")
		err = w.Write(data2)
		c.Assert(err, IsNil)
		allData.Write(data2)

		c.Assert(w.offset.Load(), Equals, int64(allData.Len()))

		err = w.Close()
		c.Assert(err, IsNil)
		c.Assert(w.file, IsNil)
		c.Assert(w.filename, Equals, "")
		c.Assert(w.offset.Load(), Equals, 0)

		c.Assert(w.Close(), IsNil) // noop

		// try to read the data back
		dataInFile, err := os.ReadFile(filename)
		c.Assert(err, IsNil)
		c.Assert(dataInFile, DeepEquals, allData.Bytes())
	}
}
