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

package simple

import (
	"github.com/pingcap/tiflow/cdc/model"
)

type decoder struct {
}

func NewDecoder() *decoder {
	//TODO implement me
	panic("implement me")
}

// AddKeyValue add the received key and values to the decoder,
func (d *decoder) AddKeyValue(key, value []byte) error {
	//TODO implement me
	panic("implement me")
}

// HasNext returns whether there is any event need to be consumed
func (d *decoder) HasNext() (model.MessageType, bool, error) {
	//TODO implement me
	panic("implement me")
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	//TODO implement me
	panic("implement me")
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	//TODO implement me
	panic("implement me")
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*model.DDLEvent, error) {
	//TODO implement me
	panic("implement me")
}
