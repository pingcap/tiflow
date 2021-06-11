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

// IsValidDataDir check if the dir is writable and readable by cdc server
func IsValidDataDir(dir string) error {
	f := filepath.Join(dir, ".writable.test")
	if err := ioutil.WriteFile(f, []byte(""), 0o600); err != nil {
		return cerror.WrapError(cerror.ErrCheckDirWritable, err)
	}

	if _, err := ioutil.ReadFile(f); err != nil {
		return cerror.WrapError(cerror.ErrCheckDirReadable, err)
	}

	return cerror.WrapError(cerror.ErrCheckDirValid, os.Remove(f))
}

// DiskInfo present the disk amount information, in bytes
type DiskInfo struct {
	All             uint64
	Used            uint64
	Free            uint64
	Avail           uint64
	AvailPercentage float32
}

// GetDiskInfo return the disk space information of the given directory.
func GetDiskInfo(dir string) (*DiskInfo, error) {
	fs := syscall.Statfs_t{}
	if err := syscall.Statfs(dir, &fs); err != nil {
		return nil, err
	}

	info := &DiskInfo{
		All:   fs.Blocks * uint64(fs.Bsize),
		Avail: fs.Bavail * uint64(fs.Bsize),
		Free:  fs.Bfree * uint64(fs.Bsize),
	}
	info.Used = info.All - info.Free
	info.AvailPercentage = float32(info.Avail) / float32(info.All) * 100

	return info, nil
}
