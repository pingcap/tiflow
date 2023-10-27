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

const (
	defaultVersion = 0
)

// EventType describes the type of the event.
type EventType string

const (
	// WatermarkType is the type of the watermark event.
	WatermarkType EventType = "WATERMARK"
)

type message struct {
	Version int `json:"version"`
	// Scheme and Table is empty for the resolved ts event.
	Schema   string    `json:"schema,omitempty"`
	Table    string    `json:"table,omitempty"`
	Type     EventType `json:"type"`
	CommitTs uint64    `json:"commitTs"`
	// Data is available for the row changed event.
	Data map[string]interface{} `json:"data,omitempty"`
	Old  map[string]interface{} `json:"old,omitempty"`
}

func newResolvedMessage(ts uint64) *message {
	return &message{
		Version:  defaultVersion,
		Type:     WatermarkType,
		CommitTs: ts,
	}
}
