// Copyright 2022 PingCAP, Inc.
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
//go:build linux

package fsutil

import (
	"os"
	"syscall"

	cerror "github.com/pingcap/tiflow/pkg/errors"
	"golang.org/x/sys/unix"
)

// PreAllocate can allocate disk space beforehand in case of ENOSPC error occurs
// when writing a file, which is not uncommon for a rigorous redolog/WAL design.
// Besides, pre-allocating disk space can reduce the overhead of file metadata updating.
func PreAllocate(f *os.File, size int64) error {
	if size == 0 {
		return nil
	}

	// fallocate is supported for xfs/ext4/btrfs, etc.
	err := unix.Fallocate(int(f.Fd()), 0, 0, size)
	if err == unix.ENOTSUP || err == syscall.EINTR {
		// if not supported, then fallback to use truncate.
		return f.Truncate(size)
	}

	if err == unix.ENOSPC {
		return cerror.ErrDiskFull
	}

	return err
}
