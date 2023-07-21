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

package internal

import (
	"encoding/json"

	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// MessageKey defines the key for a message.
type MessageKey struct {
	Ts        uint64            `json:"ts"`
	Schema    string            `json:"scm,omitempty"`
	Table     string            `json:"tbl,omitempty"`
	RowID     int64             `json:"rid,omitempty"`
	Partition *int64            `json:"ptn,omitempty"`
	Type      model.MessageType `json:"t"`
	// Only Handle Key Columns encoded in the message's value part.
	OnlyHandleKey bool `json:"ohk,omitempty"`

	// Claim check location for the message
	ClaimCheckLocation string `json:"ccl,omitempty"`
}

// Encode encodes the message key to a byte slice.
func (m *MessageKey) Encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Decode codes a message key from a byte slice.
func (m *MessageKey) Decode(data []byte) error {
	return cerror.WrapError(cerror.ErrUnmarshalFailed, json.Unmarshal(data, m))
}
