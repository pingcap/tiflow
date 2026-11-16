// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMetadataUnmarshalErrorDoesNotEchoRawValue(t *testing.T) {
	malformed := []byte(`{"secret":"credential-sentinel",`)
	for name, value := range map[string]interface{ Unmarshal([]byte) error }{
		"changefeed":        &ChangeFeedInfo{},
		"task-position":     &TaskPosition{},
		"task-status":       &TaskStatus{},
		"changefeed-status": &ChangeFeedStatus{},
		"capture":           &CaptureInfo{},
		"upstream":          &UpstreamInfo{},
	} {
		t.Run(name, func(t *testing.T) {
			err := value.Unmarshal(malformed)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "credential-sentinel")
		})
	}
}

func TestMetadataUnmarshalTimeErrorDoesNotEchoValue(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value interface{ Unmarshal([]byte) error }
		data  string
	}{
		{"changefeed-create-time", &ChangeFeedInfo{}, `{"create-time":"credential-sentinel"}`},
		{"changefeed-error-time", &ChangeFeedInfo{}, `{"error":{"time":"credential-sentinel"}}`},
		{"task-position-warning-time", &TaskPosition{}, `{"warning":{"time":"credential-sentinel"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.value.Unmarshal([]byte(tc.data))
			require.Error(t, err)
			require.NotContains(t, err.Error(), "credential-sentinel")
		})
	}
}
