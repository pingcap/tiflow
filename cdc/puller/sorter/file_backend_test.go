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

package sorter

import (
	"io"
	"os"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type fileBackendSuite struct{}

var _ = check.SerialSuites(&fileBackendSuite{})

func (s *fileBackendSuite) TestWrapIOError(c *check.C) {
	defer testleak.AfterTest(c)()

	fullFile, err := os.OpenFile("/dev/full", os.O_RDWR, 0)
	c.Assert(err, check.IsNil)
	defer fullFile.Close() //nolint:errcheck
	_, err = fullFile.WriteString("test")
	wrapped := wrapIOError(err)
	// tests that the error message gives the user some informative description
	c.Assert(wrapped, check.ErrorMatches, ".*review the settings.*no space.*")

	eof := wrapIOError(io.EOF)
	// tests that the function does not change io.EOF
	c.Assert(eof, check.Equals, io.EOF)
}

func (s *fileBackendSuite) TestNoSpace(c *check.C) {
	defer testleak.AfterTest(c)()

	fb := &fileBackEnd{
		fileName: "/dev/full",
		serde:    &msgPackGenSerde{},
	}
	w, err := fb.writer()
	c.Assert(err, check.IsNil)

	err = w.writeNext(model.NewPolymorphicEvent(generateMockRawKV(0)))
	if err == nil {
		// Due to write buffering, `writeNext` might not return an error when the filesystem is full.
		err = w.flushAndClose()
	}

	c.Assert(err, check.ErrorMatches, ".*review the settings.*no space.*")
	c.Assert(cerrors.ErrUnifiedSorterIOError.Equal(err), check.IsTrue)
}
