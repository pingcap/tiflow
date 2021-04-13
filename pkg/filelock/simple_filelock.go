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

package filelock

import (
	"os"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

const (
	cleanLockInterval = 10 * time.Second
)

// SimpleFileLock represents a file used as a lock.
type SimpleFileLock struct {
	filePath string
}

// NewSimpleFileLock creates a new SimpleFileLock by filePath
func NewSimpleFileLock(filePath string) (*SimpleFileLock, error) {
	startTime := time.Now()
	err := retry.Run(time.Millisecond*50, 20, func() error {
		lockFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o666)
		if os.IsExist(err) {
			// Conflict
			if time.Since(startTime) > cleanLockInterval {
				_ = os.Remove(filePath)
			}

			return errors.Trace(err)
		} else if err != nil {
			return backoff.Permanent(err)
		}

		// We close the file descriptor, because it is the file itself that we need.
		err = lockFile.Close()
		if err != nil {
			log.Warn("Failed to close lockFile", zap.String("path", filePath))
		}

		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &SimpleFileLock{filePath}, nil
}

// Unlock unlocks the SimpleFileLock
func (fl *SimpleFileLock) Unlock() error {
	err := os.Remove(fl.filePath)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
