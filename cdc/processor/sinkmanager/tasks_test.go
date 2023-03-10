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

package sinkmanager

import (
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestValidateAndAdjustBound(t *testing.T) {
	for _, tc := range []struct {
		name          string
		lowerBound    engine.Position
		taskTimeRange time.Duration
		expectAdjust  bool
	}{
		{
			name: "bigger than maxTaskTimeRange",
			lowerBound: engine.Position{
				StartTs:  439333515018895365,
				CommitTs: 439333515018895366,
			},
			taskTimeRange: 10 * time.Second,
			expectAdjust:  true,
		},
		{
			name: "smaller than maxTaskTimeRange",
			lowerBound: engine.Position{
				StartTs:  439333515018895365,
				CommitTs: 439333515018895366,
			},
			taskTimeRange: 1 * time.Second,
			expectAdjust:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changefeedID := model.DefaultChangeFeedID("1")
			span := spanz.TableIDToComparableSpan(1)
			lowerPhs := oracle.GetTimeFromTS(tc.lowerBound.CommitTs)
			newUpperCommitTs := oracle.GoTimeToTS(lowerPhs.Add(tc.taskTimeRange))
			upperBound := engine.GenCommitFence(newUpperCommitTs)
			newLowerBound, newUpperBound := validateAndAdjustBound(changefeedID,
				&span, tc.lowerBound, upperBound)
			if tc.expectAdjust {
				lowerPhs := oracle.GetTimeFromTS(newLowerBound.CommitTs)
				upperPhs := oracle.GetTimeFromTS(newUpperBound.CommitTs)
				require.Equal(t, maxTaskTimeRange, upperPhs.Sub(lowerPhs))
			} else {
				require.Equal(t, tc.lowerBound, newLowerBound)
				require.Equal(t, upperBound, newUpperBound)
			}
		})
	}
}
