// Copyright 2021 PingCAP, Inc.
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

/*
This file provides some common struct for unit tests an benchmark tests.
*/
package kv

import (
	"context"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tiflow/pkg/version"
	pd "github.com/tikv/pd/client"
)

type mockPDClient struct {
	pd.Client
	versionGen func() string
}

var _ pd.Client = &mockPDClient{}

func (m *mockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	s, err := m.Client.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	s.Version = m.versionGen()
	return s, nil
}

var defaultVersionGen = func() string {
	return version.MinTiKVVersion.String()
}

func mockInitializedEvent(regionID, requestID uint64) *cdcpb.ChangeDataEvent {
	initialized := &cdcpb.ChangeDataEvent{
		Events: []*cdcpb.Event{
			{
				RegionId:  regionID,
				RequestId: requestID,
				Event: &cdcpb.Event_Entries_{
					Entries: &cdcpb.Event_Entries{
						Entries: []*cdcpb.Event_Row{
							{
								Type: cdcpb.Event_INITIALIZED,
							},
						},
					},
				},
			},
		},
	}
	return initialized
}
