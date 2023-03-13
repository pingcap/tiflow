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
//go:build !linux

package fsutil

import "os"

// PreAllocate can allocate disk space beforehand in case of ENOSPC error occurs
// when writing a file, which is not uncommon for a rigorous redolog/WAL design.
// Besides, pre-allocating disk space can reduce the overhead of file metadata updating.
func PreAllocate(f *os.File, size int64) error {
	// if the OS is not linux, use truncate as a fallback.
	return f.Truncate(size)
}
