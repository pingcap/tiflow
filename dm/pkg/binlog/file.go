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

package binlog

import (
	"os"
	"sort"

	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// ReadSortedBinlogFromDir reads and returns all binlog files (sorted ascending by binlog filename and sequence number).
func ReadSortedBinlogFromDir(dirpath string) ([]string, error) {
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, terror.ErrReadDir.Delegate(err, dirpath)
	}
	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, terror.ErrReadDir.Delegate(err, dirpath)
	}
	if len(names) == 0 {
		return nil, nil
	}

	// sorting bin.100000, ..., bin.1000000, ..., bin.999999
	type tuple struct {
		filename string
		parsed   Filename
	}
	tmp := make([]tuple, 0, len(names)-1)

	for _, f := range names {
		p, err2 := ParseFilename(f)
		if err2 != nil {
			// may contain some file that can't be parsed, like relay meta. ignore them
			log.L().Info("collecting binlog file, ignore invalid file", zap.String("file", f))
			continue
		}
		tmp = append(tmp, tuple{
			filename: f,
			parsed:   p,
		})
	}

	sort.Slice(tmp, func(i, j int) bool {
		if tmp[i].parsed.BaseName != tmp[j].parsed.BaseName {
			return tmp[i].parsed.BaseName < tmp[j].parsed.BaseName
		}
		return tmp[i].parsed.LessThan(tmp[j].parsed)
	})

	ret := make([]string, len(tmp))
	for i := range tmp {
		ret[i] = tmp[i].filename
	}

	return ret, nil
}
