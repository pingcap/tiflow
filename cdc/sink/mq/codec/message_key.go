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

package codec

import (
	"encoding/json"

	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type mqMessageKey struct {
	Ts        uint64            `json:"ts"`
	Schema    string            `json:"scm,omitempty"`
	Table     string            `json:"tbl,omitempty"`
	RowID     int64             `json:"rid,omitempty"`
	Partition *int64            `json:"ptn,omitempty"`
	Type      model.MessageType `json:"t"`
}

func (m *mqMessageKey) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *mqMessageKey) decode(data []byte) error {
	return cerror.WrapError(cerror.ErrUnmarshalFailed, json.Unmarshal(data, m))
}
