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

package kv

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/stretchr/testify/require"
)

func TestGetChangeDataEventCommitTs(t *testing.T) {
	cevents := []*cdcpb.ChangeDataEvent{
		{},
		{
			Events: []*cdcpb.Event{
				{Event: &cdcpb.Event_Admin_{}},
			},
		},
		{
			Events: []*cdcpb.Event{
				{Event: &cdcpb.Event_Entries_{Entries: &cdcpb.Event_Entries{}}},
			},
		},
	}
	for _, cevent := range cevents {
		require.Equal(t, uint64(0), getChangeDataEventCommitTs(cevent))
	}
}
