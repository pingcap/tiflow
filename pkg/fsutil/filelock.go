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

package fsutil

import (
	"os"
	"syscall"

	"github.com/pingcap/errors"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
)

// FileLock represents a file lock created by `flock`.
// For more on the system call `flock`, read:
// https://linux.die.net/man/2/flock
type FileLock struct {
	fd *os.File
}

// NewFileLock creates a new file lock on the file described in filePath.
func NewFileLock(filePath string) (*FileLock, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &FileLock{fd: file}, nil
}

// Lock places a lock on the file
func (fl *FileLock) Lock() error {
	fdInt := fl.fd.Fd()
	err := syscall.Flock(int(fdInt), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		if err == syscall.EAGAIN {
			return cerrors.ErrConflictingFileLocks.Wrap(err).GenWithStackByArgs(fl.fd.Name())
		}
		return errors.Trace(err)
	}
	return nil
}

// Unlock unlocks the file lock
func (fl *FileLock) Unlock() error {
	fdInt := fl.fd.Fd()
	err := syscall.Flock(int(fdInt), syscall.LOCK_UN)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// Close closes the underlying file.
// NOTE: this function will unlock the lock as well.
func (fl *FileLock) Close() error {
	err := fl.fd.Close()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// IsLocked checks whether the file is currently locked
func (fl *FileLock) IsLocked() (bool, error) {
	fdInt := fl.fd.Fd()
	err := syscall.Flock(int(fdInt), syscall.LOCK_SH|syscall.LOCK_NB)
	if err == syscall.EAGAIN {
		return true, nil
	} else if err != nil {
		return false, errors.Trace(err)
	}
	return false, nil
}
