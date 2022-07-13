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

package common

import (
	"encoding/binary"
	"math"

	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pkg/errors"
)

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
	CheckPointTs   uint64
	ResolvedTsList map[int64]uint64
}

// ResolvedTs returns the minimal resolved timestamp of all tables.
// If no resolved timestamp is record, return checkpoint instead.
func (z LogMeta) ResolvedTs() model.Ts {
	var minResolvedTs uint64 = math.MaxUint64
	for _, ts := range z.ResolvedTsList {
		if ts < minResolvedTs {
			minResolvedTs = ts
		}
	}
	if minResolvedTs == math.MaxUint64 {
		return z.CheckPointTs
	}
	return minResolvedTs
}

// MarshalMsg implements msgp.Marshaler
func (z LogMeta) MarshalMsg(b []byte) ([]byte, error) {
	buff := make([]byte, 16+16*len(z.ResolvedTsList))
	binary.LittleEndian.PutUint64(buff[0:8], z.CheckPointTs)
	binary.LittleEndian.PutUint64(buff[8:16], uint64(len(z.ResolvedTsList)))

	tStart := 16
	for tID, ts := range z.ResolvedTsList {
		binary.LittleEndian.PutUint64(buff[tStart:tStart+8], uint64(tID))
		binary.LittleEndian.PutUint64(buff[tStart+8:tStart+16], ts)
		tStart += 16
	}
	b = append(b, buff...)
	return b, nil
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *LogMeta) UnmarshalMsg(b []byte) ([]byte, error) {
	if len(b) < 16 {
		return b, cerror.WrapError(cerror.ErrUnmarshalFailed, errors.New("unmarshal redo meta"))
	}
	z.CheckPointTs = binary.LittleEndian.Uint64(b[0:8])
	tsListLen := binary.LittleEndian.Uint64(b[8:16])
	z.ResolvedTsList = make(map[model.TableID]model.Ts, tsListLen)
	if len(b) < 16+16*int(tsListLen) {
		return b, cerror.WrapError(cerror.ErrUnmarshalFailed, errors.New("unmarshal redo meta"))
	}

	tStart := 16
	for i := 0; i < int(tsListLen); i++ {
		tID := int64(binary.LittleEndian.Uint64(b[tStart : tStart+8]))
		ts := binary.LittleEndian.Uint64(b[tStart+8 : tStart+16])
		z.ResolvedTsList[tID] = ts
		tStart += 16
	}
	return b[16*tsListLen+16:], nil
}
