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

package model

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// CaptureID is the type for capture ID
type CaptureID = string

const DummyCaptureID CaptureID = ""

type CaptureStatus int

const (
	CaptureStatusHealthy CaptureStatus = iota
	CaptureStatusDraining
	CaptureStatusClosing
)

func (s CaptureStatus) String() string {
	switch s {
	case CaptureStatusHealthy:
		return "healthy"
	case CaptureStatusDraining:
		return "draining"
	case CaptureStatusClosing:
		return "closing"
	}
	panic("unreachable")
}

// CaptureInfo store in etcd.
type CaptureInfo struct {
	ID            CaptureID     `json:"id"`
	AdvertiseAddr string        `json:"address"`
	Version       string        `json:"version"`
	Status        CaptureStatus `json:"status"`
}

// NewCaptureInfo return the basic capture info.
func NewCaptureInfo(addr, version string) *CaptureInfo {
	return &CaptureInfo{
		ID:            uuid.New().String(),
		AdvertiseAddr: addr,
		Version:       version,
		Status:        CaptureStatusHealthy,
	}
}

// Marshal using json.Marshal.
func (c *CaptureInfo) Marshal() ([]byte, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMarshalFailed, err)
	}

	return data, nil
}

// Unmarshal from binary data.
func (c *CaptureInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, c)
	return errors.Annotatef(cerror.WrapError(cerror.ErrUnmarshalFailed, err),
		"unmarshal data: %v", data)
}

// ListVersionsFromCaptureInfos returns the version list of the CaptureInfo list.
func ListVersionsFromCaptureInfos(captureInfos []*CaptureInfo) []string {
	var captureVersions []string
	for _, ci := range captureInfos {
		captureVersions = append(captureVersions, ci.Version)
	}

	return captureVersions
}
