// Copyright 2019 PingCAP, Inc.
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

package model

import (
	"encoding/json"

	"github.com/pingcap/errors"
)

// CaptureInfo store in etcd.
type CaptureInfo struct {
	ID            CaptureID `json:"id"`
	AdvertiseAddr string    `json:"address"`
}

// Marshal using json.Marshal.
func (c *CaptureInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *CaptureInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}
