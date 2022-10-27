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

package utils

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"go.uber.org/zap"
)

const (
	// the binlog file name format is `base + '.' + seq`.
	binlogFilenameSep = "."
	// PosRelaySubDirSuffixSeparator is used to differ binlog position from multiple
	// (switched) masters, we added a suffix which comes from relay log subdirectory
	// into binlogPos.Name. And we also need support position with RelaySubDirSuffix
	// should always > position without RelaySubDirSuffix, so we can continue from
	// latter to former automatically. convertedPos.BinlogName =
	//   originalPos.BinlogBaseName + PosRelaySubDirSuffixSeparator + RelaySubDirSuffix + binlogFilenameSep + originalPos.BinlogSeq
	// eg. mysql-bin.000003 under folder c6ae5afe-c7a3-11e8-a19d-0242ac130006.000002 => mysql-bin|000002.000003
	// when new relay log subdirectory is created, RelaySubDirSuffix should increase.
	PosRelaySubDirSuffixSeparator = "|"
)

// Filename represents a binlog filename.
type Filename struct {
	BaseName string
	Seq      string
	SeqInt64 int64
}

// LessThan checks whether this filename < other filename.
func (f Filename) LessThan(other Filename) bool {
	return f.BaseName == other.BaseName && f.SeqInt64 < other.SeqInt64
}

// GreaterThanOrEqualTo checks whether this filename >= other filename.
func (f Filename) GreaterThanOrEqualTo(other Filename) bool {
	return f.BaseName == other.BaseName && f.SeqInt64 >= other.SeqInt64
}

// GreaterThan checks whether this filename > other filename.
func (f Filename) GreaterThan(other Filename) bool {
	return f.BaseName == other.BaseName && f.SeqInt64 > other.SeqInt64
}

// ParseFilename parses a string representation binlog filename into a `Filename`.
func ParseFilename(filename string) (Filename, error) {
	var fn Filename
	parts := strings.Split(filename, binlogFilenameSep)
	if len(parts) != 2 {
		return fn, terror.Annotatef(terror.ErrBinlogInvalidFilename.Generate(), "filename %s", filename)
	}

	var (
		seqInt64 int64
		err      error
	)
	if seqInt64, err = strconv.ParseInt(parts[1], 10, 64); err != nil || seqInt64 <= 0 {
		return fn, terror.Annotatef(terror.ErrBinlogInvalidFilename.Generate(), "filename %s", filename)
	}
	fn.BaseName = parts[0]
	fn.Seq = parts[1]
	fn.SeqInt64 = seqInt64
	return fn, nil
}

// VerifyFilename verifies whether is a valid MySQL/MariaDB binlog filename.
// valid format is `base + '.' + seq`.
func VerifyFilename(filename string) bool {
	if _, err := ParseFilename(filename); err != nil {
		return false
	}
	return true
}

// GetFilenameIndex returns a int64 index value (seq number) of the filename.
func GetFilenameIndex(filename string) (int64, error) {
	fn, err := ParseFilename(filename)
	if err != nil {
		return 0, err
	}
	return fn.SeqInt64, nil
}

// ConstructFilename constructs a binlog filename from the basename and seq.
func ConstructFilename(baseName, seq string) string {
	return fmt.Sprintf("%s%s%s", baseName, binlogFilenameSep, seq)
}

// ConstructFilenameWithUUIDSuffix constructs a binlog filename with UUID suffix.
func ConstructFilenameWithUUIDSuffix(originalName Filename, uuidSuffix string) string {
	return fmt.Sprintf("%s%s%s%s%s", originalName.BaseName, PosRelaySubDirSuffixSeparator, uuidSuffix, binlogFilenameSep, originalName.Seq)
}

// SplitFilenameWithUUIDSuffix analyzes a binlog filename with UUID suffix.
func SplitFilenameWithUUIDSuffix(filename string) (baseName, uuidSuffix, seq string, err error) {
	items1 := strings.Split(filename, PosRelaySubDirSuffixSeparator)
	if len(items1) != 2 {
		return "", "", "", terror.ErrBinlogInvalidFilenameWithUUIDSuffix.Generate(filename)
	}

	baseName = items1[0]
	items2 := strings.Split(items1[1], binlogFilenameSep)

	if len(items2) != 2 {
		return "", "", "", terror.ErrBinlogInvalidFilenameWithUUIDSuffix.Generate(filename)
	}
	uuidSuffix = items2[0]
	seq = items2[1]
	return baseName, uuidSuffix, seq, nil
}

// ExtractRealName removes relay log uuid suffix if it exists and returns real binlog name.
func ExtractRealName(name string) string {
	if !strings.Contains(name, PosRelaySubDirSuffixSeparator) {
		return name
	}
	baseName, _, seq, err := SplitFilenameWithUUIDSuffix(name)
	if err != nil {
		log.L().Error("failed to split binlog name with uuid suffix", zap.String("name", name), zap.Error(err))
		// nolint:nilerr
		return name
	}
	return ConstructFilename(baseName, seq)
}
