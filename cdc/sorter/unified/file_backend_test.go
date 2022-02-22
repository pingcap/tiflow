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

package unified

import (
	"io"
	"os"
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/encoding"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestWrapIOError(t *testing.T) {
	fullFile, err := os.OpenFile("/dev/full", os.O_RDWR, 0)
	require.Nil(t, err)
	defer fullFile.Close() //nolint:errcheck

	_, err = fullFile.WriteString("test")
	wrapped := wrapIOError(err)
	// tests that the error message gives the user some informative description
	require.Regexp(t, wrapped, ".*review the settings.*no space.*")

	eof := wrapIOError(io.EOF)
	// tests that the function does not change io.EOF
	require.Equal(t, io.EOF, eof)
}

func TestNoSpace(t *testing.T) {
	fb := &fileBackEnd{
		fileName: "/dev/full",
		serde:    &encoding.MsgPackGenSerde{},
	}
	w, err := fb.writer()
	require.Nil(t, err)

	err = w.writeNext(model.NewPolymorphicEvent(generateMockRawKV(0)))
	if err == nil {
		// Due to write buffering, `writeNext` might not return an error when the filesystem is full.
		err = w.flushAndClose()
	}

	require.Regexp(t, err, ".*review the settings.*no space.*")
	require.True(t, cerrors.ErrUnifiedSorterIOError.Equal(err))
}
