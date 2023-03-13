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
//
//go:build freebsd

package fsutil

import (
	"os"
	"path/filepath"
	"syscall"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// GetDiskInfo return the disk space information of the given directory
// the caller should guarantee that dir exist
func GetDiskInfo(dir string) (*DiskInfo, error) {
	f := filepath.Join(dir, "file.test")
	if err := os.WriteFile(f, []byte(""), 0o600); err != nil {
		return nil, cerror.WrapError(cerror.ErrGetDiskInfo, err)
	}

	fs := syscall.Statfs_t{}
	if err := syscall.Statfs(dir, &fs); err != nil {
		return nil, cerror.WrapError(cerror.ErrGetDiskInfo, err)
	}

	info := &DiskInfo{
		All:   fs.Blocks * uint64(fs.Bsize) / gb,
		Avail: uint64(fs.Bavail) * uint64(fs.Bsize) / gb,
		Free:  fs.Bfree * uint64(fs.Bsize) / gb,
	}
	info.Used = info.All - info.Free
	info.AvailPercentage = float32(info.Avail) / float32(info.All) * 100

	if err := os.Remove(f); err != nil {
		if !os.IsNotExist(err) {
			return info, cerror.WrapError(cerror.ErrGetDiskInfo, err)
		}
	}

	return info, nil
}
