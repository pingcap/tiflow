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
	// MinSectorSize ...
	MinSectorSize = 512

	// TmpEXT ...
	TmpEXT = ".tmp"
	// LogEXT ...
	LogEXT = ".log"
	// MetaEXT ...
	MetaEXT = ".meta"
	// MetaTmpEXT ...
	MetaTmpEXT = ".mtmp"
	// SortLogEXT ...
	SortLogEXT = ".sort"

	// DefaultFileMode ...
	DefaultFileMode = 0o644
)

const (
	// DefaultMetaFileType ...
	DefaultMetaFileType = "meta"
	// DefaultRowLogFileType ...
	DefaultRowLogFileType = "row"
	// DefaultDDLLogFileType ...
	DefaultDDLLogFileType = "ddl"
)

// LogMeta ...
type LogMeta struct {
	CheckPointTs   uint64           `msg:"checkPointTs"`
	ResolvedTs     uint64           `msg:"resolvedTs"`
	ResolvedTsList map[int64]uint64 `msg:"resolvedTsList"`
}
