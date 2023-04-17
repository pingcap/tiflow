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

package avro

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec"
)

type decoder struct {
	key   []byte
	value []byte

	enableTiDBExtension    bool
	enableRowLevelChecksum bool

	keySchemaM   *schemaManager
	valueSchemaM *schemaManager
}

func NewDecoder(key, value []byte,
	enableTiDBExtension bool,
	enableRowLevelChecksum bool,
) codec.RowEventDecoder {
	return &decoder{
		key:                    key,
		value:                  value,
		enableTiDBExtension:    enableTiDBExtension,
		enableRowLevelChecksum: enableRowLevelChecksum,
	}
}

func (d *decoder) HasNext() (model.MessageType, bool, error) {

}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {

}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {

}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*model.DDLEvent, error) {

}
