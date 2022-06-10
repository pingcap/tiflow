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

package model

import (
	"encoding/json"

	"github.com/pingcap/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// UpstreamID is the type for upstream ID
type UpstreamID = uint64

// UpstreamInfo store in etcd.
type UpstreamInfo struct {
	ID            uint64   `json:"id"`
	PDEndpoints   string   `json:"pd-endpoints"`
	KeyPath       string   `json:"key-path"`
	CertPath      string   `json:"cert-path"`
	CAPath        string   `json:"ca-path"`
	CertAllowedCN []string `json:"cert-allowed-cn"`
}

// Marshal using json.Marshal.
func (c *UpstreamInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *UpstreamInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(cerror.WrapError(cerror.ErrUnmarshalFailed, err),
		"unmarshal data: %v", data)
}

// Clone returns a cloned upstreamInfo
func (c *UpstreamInfo) Clone() (*UpstreamInfo, error) {
	s, err := c.Marshal()
	if err != nil {
		return nil, err
	}
	cloned := new(UpstreamInfo)
	err = cloned.Unmarshal(s)
	return cloned, err
}
