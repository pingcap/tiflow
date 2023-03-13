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

	"golang.org/x/sys/unix"
)

// DropPageCache us fadvise to tell OS the reclaim memory associated with the file
// as soon as possible, which can avoid the unexpected I/O latency spike risk.
func DropPageCache(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()

	err = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	return err
}
