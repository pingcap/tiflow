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
	"path/filepath"
	"syscall"

	"github.com/pingcap/errors"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// IsDirAndWritable checks a given path is directory and writable
func IsDirAndWritable(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		return cerror.WrapError(cerror.ErrCheckDirWritable, err)
	}
	if !st.IsDir() {
		return cerror.WrapError(cerror.ErrCheckDirWritable, errors.Errorf("%s is not a directory", path))
	}
	return IsDirWritable(path)
}

// IsDirWritable checks if a dir is writable, return error nil means it is writable
func IsDirWritable(dir string) error {
	f := filepath.Join(dir, ".writable.test")
	if err := ioutil.WriteFile(f, []byte(""), 0o600); err != nil {
		return cerror.WrapError(cerror.ErrCheckDirWritable, err)
	}
	return cerror.WrapError(cerror.ErrCheckDirWritable, os.Remove(f))
}

// GetDiskAvailableSpace return the available space of the specified dir
// the caller should guarantee that dir is a valid directory
func GetDiskAvailableSpace(dir string) (int32, error) {
	fs := syscall.Statfs_t{}
	if err := syscall.Statfs(dir, &fs); err != nil {
		return 0, cerror.WrapError(cerror.ErrGetDiskAvailableSpace, err)
	}

	available := fs.Bavail * uint64(fs.Bsize)
	return int32(available / 1024 / 1024 / 1024), nil
}
