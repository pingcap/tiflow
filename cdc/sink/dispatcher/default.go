// Copyright 2020 PingCAP, Inc.
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

package dispatcher

import (
	"encoding/json"
	"hash/crc32"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"go.uber.org/zap"
)

type defaultDispatcher struct {
	partitionNum int32
}

func (d *defaultDispatcher) Dispatch(row *model.RowChangedEvent) int32 {
	hash := crc32.NewIEEE()
	if len(row.IndieMarkCol) == 0 {
		// distribute partition by table
		_, err := hash.Write([]byte(row.Table.Schema))
		if err != nil {
			log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
		}
		_, err = hash.Write([]byte(row.Table.Table))
		if err != nil {
			log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
		}
		return int32(hash.Sum32() % uint32(d.partitionNum))
	}
	// FIXME(leoppro): if the row events includes both pre-cols and cols
	// the dispatch logic here is wrong

	// distribute partition by rowid or unique column value
	dispatchCols := row.Columns
	if len(row.Columns) == 0 {
		dispatchCols = row.PreColumns
	}
	value := dispatchCols[row.IndieMarkCol].Value
	b, err := json.Marshal(value)
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}
	_, err = hash.Write(b)
	if err != nil {
		log.Fatal("calculate hash of message key failed, please report a bug", zap.Error(err))
	}
	return int32(hash.Sum32() % uint32(d.partitionNum))
}
