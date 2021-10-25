//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

//go:generate msgp

package common

const (
	// MinSectorSize is minimum sector size used when flushing log so that log can safely
	// distinguish between torn writes and ordinary data corruption.
	MinSectorSize = 512
)

const (
	// TmpEXT is the file ext of log file before safely wrote to disk
	TmpEXT = ".tmp"
	// LogEXT is the file ext of log file after safely wrote to disk
	LogEXT = ".log"
	// MetaEXT is the meta file ext of meta file after safely wrote to disk
	MetaEXT = ".meta"
	// MetaTmpEXT is the meta file ext of meta file before safely wrote to disk
	MetaTmpEXT = ".mtmp"
	// SortLogEXT is the sorted log file ext of log file after safely wrote to disk
	SortLogEXT = ".sort"
)

const (
	// DefaultFileMode is the default mode when operation files
	DefaultFileMode = 0o644
	// DefaultDirMode is the default mode when operation dir
	DefaultDirMode = 0o755
)

const (
	// DefaultMetaFileType is the default file type of meta file
	DefaultMetaFileType = "meta"
	// DefaultRowLogFileType is the default file type of row log file
	DefaultRowLogFileType = "row"
	// DefaultDDLLogFileType is the default file type of ddl log file
	DefaultDDLLogFileType = "ddl"
)

// LogMeta is used for store meta info.
type LogMeta struct {
	CheckPointTs   uint64           `msg:"checkPointTs"`
	ResolvedTs     uint64           `msg:"resolvedTs"`
	ResolvedTsList map[int64]uint64 `msg:"-"`
}
