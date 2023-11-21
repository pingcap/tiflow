// Copyright 2023 PingCAP, Inc.
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
	"encoding/json"

	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type decoder struct {
	value []byte

	msg *message
}

// NewDecoder returns a new decoder
func NewDecoder() *decoder {
	return &decoder{}
}

// AddKeyValue add the received key and values to the decoder,
func (d *decoder) AddKeyValue(_, value []byte) error {
	if d.value != nil {
		return cerror.ErrDecodeFailed.GenWithStack(
			"decoder value already exists, not consumed yet")
	}
	d.value = value
	return nil
}

// HasNext returns whether there is any event need to be consumed
func (d *decoder) HasNext() (model.MessageType, bool, error) {
	if d.value == nil {
		return model.MessageTypeUnknown, false, nil
	}

	var m message
	if err := json.Unmarshal(d.value, &m); err != nil {
		return model.MessageTypeUnknown, false, cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	d.msg = &m
	d.value = nil

	if d.msg.Type == WatermarkType {
		return model.MessageTypeResolved, true, nil
	}
	if d.msg.Type == DDLType {
		return model.MessageTypeDDL, true, nil
	}
	if d.msg.Data != nil || d.msg.Old != nil {
		return model.MessageTypeRow, true, nil
	}

	return model.MessageTypeUnknown, false, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	if d.msg.Type != WatermarkType {
		return 0, cerror.ErrCodecDecode.GenWithStack(
			"not found resolved event message")
	}

	ts := d.msg.CommitTs
	d.msg = nil

	return ts, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if d.msg == nil || (d.msg.Data == nil && d.msg.Old == nil) {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"not found row changed event message")
	}
	event := buildRowChangedEvent(d.msg)
	d.msg = nil
	return event, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*model.DDLEvent, error) {
	if d.msg.Type != DDLType && d.msg.Type != BootstrapType {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"not found ddl event message")
	}

	ddl := newDDLEvent(d.msg)
	d.msg = nil

	return ddl, nil
}
