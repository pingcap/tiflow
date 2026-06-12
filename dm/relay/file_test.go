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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestCollectBinlogFiles(t *testing.T) {
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
	require.Error(t, err)
	require.Nil(t, files)

	dir := t.TempDir()

	// create all valid binlog files
	for _, fn := range valid {
		err = os.WriteFile(filepath.Join(dir, fn), nil, 0o600)
		require.NoError(t, err)
	}
	files, err = CollectAllBinlogFiles(dir)
	require.NoError(t, err)
	require.Equal(t, valid, files)

	// create some invalid binlog files
	for _, fn := range invalid {
		err = os.WriteFile(filepath.Join(dir, fn), nil, 0o600)
		require.NoError(t, err)
	}
	files, err = CollectAllBinlogFiles(dir)
	require.NoError(t, err)
	require.Equal(t, valid, files)

	// create some invalid meta files
	for _, fn := range meta {
		err = os.WriteFile(filepath.Join(dir, fn), nil, 0o600)
		require.NoError(t, err)
	}
	files, err = CollectAllBinlogFiles(dir)
	require.NoError(t, err)
	require.Equal(t, valid, files)

	// collect newer files, none
	files, err = CollectBinlogFilesCmp(dir, valid[len(valid)-1], FileCmpBigger)
	require.NoError(t, err)
	require.Equal(t, []string{}, files)

	// collect newer files, some
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpBigger)
	require.NoError(t, err)
	require.Equal(t, valid[1:], files)

	// collect newer or equal files, all
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpBiggerEqual)
	require.NoError(t, err)
	require.Equal(t, valid, files)

	// collect newer or equal files, some
	files, err = CollectBinlogFilesCmp(dir, valid[1], FileCmpBiggerEqual)
	require.NoError(t, err)
	require.Equal(t, valid[1:], files)

	// collect older files, none
	files, err = CollectBinlogFilesCmp(dir, valid[0], FileCmpLess)
	require.NoError(t, err)
	require.Equal(t, []string{}, files)

	// collect older files, some
	files, err = CollectBinlogFilesCmp(dir, valid[len(valid)-1], FileCmpLess)
	require.NoError(t, err)
	require.Equal(t, valid[:len(valid)-1], files)
}

func TestCollectBinlogFilesCmp(t *testing.T) {
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
	require.True(t, terror.ErrEmptyRelayDir.Equal(err))
	require.Nil(t, files)

	// empty base filename, not found
	dir = t.TempDir()
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	require.True(t, errors.IsNotFound(err))
	require.Nil(t, files)

	// base file not found
	baseFile = utils.MetaFilename
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	require.True(t, errors.IsNotFound(err))
	require.Nil(t, files)

	// create a meta file
	filename := filepath.Join(dir, utils.MetaFilename)
	err = os.WriteFile(filename, nil, 0o600)
	require.NoError(t, err)

	// invalid base filename, is a meta filename
	files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
	require.Error(t, err)
	require.Regexp(t, ".*invalid binlog filename.*", err.Error())
	require.Nil(t, files)

	// create some binlog files
	for _, f := range binlogFiles {
		filename = filepath.Join(dir, f)
		err = os.WriteFile(filename, nil, 0o600)
		require.NoError(t, err)
	}

	// > base file
	cmp = FileCmpBigger
	var i int
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		require.NoError(t, err)
		require.Equal(t, binlogFiles[i+1:], files)
	}

	// >= base file
	cmp = FileCmpBiggerEqual
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		require.NoError(t, err)
		require.Equal(t, binlogFiles[i:], files)
	}

	// < base file
	cmp = FileCmpLess
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		require.NoError(t, err)
		require.Equal(t, binlogFiles[:i], files)
	}

	// add a basename mismatch binlog file
	filename = filepath.Join(dir, "bin-mysql.100000")
	err = os.WriteFile(filename, nil, 0o600)
	require.NoError(t, err)

	// test again, should ignore it
	for i, baseFile = range binlogFiles {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		require.NoError(t, err)
		require.Equal(t, binlogFiles[:i], files)
	}

	// other cmp not supported yet
	cmps := []FileCmp{FileCmpLessEqual, FileCmpEqual}
	for _, cmp = range cmps {
		files, err = CollectBinlogFilesCmp(dir, baseFile, cmp)
		require.Error(t, err)
		require.Regexp(t, ".*not supported.*", err.Error())
		require.Nil(t, files)
	}
}

func TestGetFirstBinlogName(t *testing.T) {
	var (
		baseDir = t.TempDir()
		uuid    = "b60868af-5a6f-11e9-9ea3-0242ac160006.000001"
		subDir  = filepath.Join(baseDir, uuid)
	)

	// sub directory not exist
	name, err := getFirstBinlogName(baseDir, uuid)
	require.Error(t, err)
	require.Regexp(t, ".*(no such file or directory|The system cannot find the file specified).*", err.Error())
	require.Equal(t, "", name)

	// empty directory
	err = os.MkdirAll(subDir, 0o700)
	require.NoError(t, err)
	name, err = getFirstBinlogName(baseDir, uuid)
	require.Error(t, err)
	require.Regexp(t, ".*not found.*", err.Error())
	require.Equal(t, "", name)

	// has file, but not a valid binlog file. Now the error message is binlog files not found
	filename := "invalid.bin"
	err = os.WriteFile(filepath.Join(subDir, filename), nil, 0o600)
	require.NoError(t, err)
	_, err = getFirstBinlogName(baseDir, uuid)
	require.Error(t, err)
	require.Regexp(t, ".*not found.*", err.Error())
	err = os.Remove(filepath.Join(subDir, filename))
	require.NoError(t, err)

	// has a valid binlog file
	filename = "z-mysql-bin.000002" // z prefix, make it become not the _first_ if possible.
	err = os.WriteFile(filepath.Join(subDir, filename), nil, 0o600)
	require.NoError(t, err)
	name, err = getFirstBinlogName(baseDir, uuid)
	require.NoError(t, err)
	require.Equal(t, filename, name)

	// has one more earlier binlog file
	filename = "z-mysql-bin.000001"
	err = os.WriteFile(filepath.Join(subDir, filename), nil, 0o600)
	require.NoError(t, err)
	name, err = getFirstBinlogName(baseDir, uuid)
	require.NoError(t, err)
	require.Equal(t, filename, name)

	// has a meta file
	err = os.WriteFile(filepath.Join(subDir, utils.MetaFilename), nil, 0o600)
	require.NoError(t, err)
	name, err = getFirstBinlogName(baseDir, uuid)
	require.NoError(t, err)
	require.Equal(t, filename, name)
}

func TestFileSizeUpdated(t *testing.T) {
	var (
		filename   = "mysql-bin.000001"
		filePath   = filepath.Join(t.TempDir(), filename)
		data       = []byte("meaningless file content")
		latestSize = int64(len(data))
	)

	// file not exists
	cmp, err := fileSizeUpdated(filePath, latestSize)
	require.Error(t, err)
	require.Regexp(t, ".*(no such file or directory|The system cannot find the file specified).*", err.Error())
	require.Equal(t, 0, cmp)

	// create and write the file
	err = os.WriteFile(filePath, data, 0o600)
	require.NoError(t, err)

	// equal
	cmp, err = fileSizeUpdated(filePath, latestSize)
	require.NoError(t, err)
	require.Equal(t, 0, cmp)

	// less than
	cmp, err = fileSizeUpdated(filePath, latestSize+1)
	require.NoError(t, err)
	require.Equal(t, -1, cmp)

	// greater than
	cmp, err = fileSizeUpdated(filePath, latestSize-1)
	require.NoError(t, err)
	require.Equal(t, 1, cmp)
}
