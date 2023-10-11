// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package partition

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestKeyDispatcher_DispatchRowChangedEvent(t *testing.T) {
	tests := []struct {
		name          string
		partitionKey  string
		rowChangedEvt *model.RowChangedEvent
		wantPartition int32
		wantKey       string
	}{
		{
			name:          "dispatch to partition 0 with partition key 'foo'",
			partitionKey:  "foo",
			rowChangedEvt: &model.RowChangedEvent{},
			wantPartition: 0,
			wantKey:       "foo",
		},
		// Add more test cases here
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewKeyDispatcher(tt.partitionKey)
			gotPartition, gotKey, err := d.DispatchRowChangedEvent(tt.rowChangedEvt, 0)
			require.NoError(t, err)
			if gotPartition != tt.wantPartition {
				t.Errorf("DispatchRowChangedEvent() gotPartition = %v, want %v", gotPartition, tt.wantPartition)
			}
			if gotKey != tt.wantKey {
				t.Errorf("DispatchRowChangedEvent() gotKey = %v, want %v", gotKey, tt.wantKey)
			}
		})
	}
}
