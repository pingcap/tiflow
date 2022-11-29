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
	"testing"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type testBinlogWriterSuite struct {
	suite.Suite
}

func TestBinlogWriterSuite(t *testing.T) {
	suite.Run(t, new(testBinlogWriterSuite))
}

func (t *testBinlogWriterSuite) TestWrite() {
	dir := t.T().TempDir()
	uuid := "3ccc475b-2343-11e7-be21-6c0b84d59f30.000001"
	binlogDir := filepath.Join(dir, uuid)
	require.NoError(t.T(), os.Mkdir(binlogDir, 0o755))

	filename := "test-mysql-bin.000001"
	var (
		allData bytes.Buffer
		data1   = []byte("test-data")
	)

	{
		w := NewBinlogWriter(log.L(), dir)
		require.NotNil(t.T(), w)
		require.NoError(t.T(), w.Open(uuid, filename))
		fwStatus := w.Status()
		require.Equal(t.T(), filename, fwStatus.Filename)
		require.Equal(t.T(), int64(allData.Len()), fwStatus.Offset)
		fwStatusStr := fwStatus.String()
		require.Contains(t.T(), fwStatusStr, filename)
		require.NoError(t.T(), w.Close())
	}

	{
		// not opened
		w := NewBinlogWriter(log.L(), dir)
		err := w.Write(data1)
		require.Contains(t.T(), err.Error(), "no underlying writer opened")

		// open non exist dir
		err = w.Open("not-exist-uuid", "bin.000001")
		require.Contains(t.T(), err.Error(), "no such file or directory")
	}

	{
		// normal call flow
		w := NewBinlogWriter(log.L(), dir)
		err := w.Open(uuid, filename)
		require.NoError(t.T(), err)
		require.NotNil(t.T(), w.file)
		require.Equal(t.T(), filename, w.filename.Load())
		require.Equal(t.T(), int64(0), w.offset.Load())

		err = w.Write(data1)
		require.NoError(t.T(), err)
		err = w.Flush()
		require.NoError(t.T(), err)
		allData.Write(data1)

		fwStatus := w.Status()
		require.Equal(t.T(), fwStatus.Filename, w.filename.Load())
		require.Equal(t.T(), int64(len(data1)), fwStatus.Offset)

		// write data again
		data2 := []byte("another-data")
		err = w.Write(data2)
		require.NoError(t.T(), err)
		allData.Write(data2)

		require.LessOrEqual(t.T(), int64(allData.Len()), w.offset.Load())

		err = w.Close()
		require.NoError(t.T(), err)
		require.Nil(t.T(), w.file)
		require.Equal(t.T(), "", w.filename.Load())
		require.Equal(t.T(), int64(0), w.offset.Load())

		// try to read the data back
		fullName := filepath.Join(binlogDir, filename)
		dataInFile, err := os.ReadFile(fullName)
		require.NoError(t.T(), err)
		require.Equal(t.T(), allData.Bytes(), dataInFile)
	}

	{
		// cover for error
		w := NewBinlogWriter(log.L(), dir)
		err := w.Open(uuid, filename)
		require.NoError(t.T(), err)
		require.NotNil(t.T(), w.file)

		err = w.Write(data1)
		require.NoError(t.T(), err)
		err = w.Flush()
		require.NoError(t.T(), err)

		require.NoError(t.T(), w.file.Close())
		// write data again
		data2 := []byte("another-data")
		// we cannot determine the error is caused by `Write` or `Flush`
		// nolint:errcheck
		w.Write(data2)
		// nolint:errcheck
		w.Flush()
		require.True(t.T(), terror.ErrBinlogWriterWriteDataLen.Equal(w.Close()))
	}
}
