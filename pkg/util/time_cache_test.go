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

package util

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

// MockPDClient mocks pd.Client to facilitate unit testing.
type MockPDClient struct {
	pd.Client
}

// GetTS implements pd.Client.GetTS.
func (m *MockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, nil
}

func TestTimeFromPD(t *testing.T) {
	mockPDClient := &MockPDClient{}
	pdTimeCache := NewPDTimeCache(mockPDClient)
	ctx := context.Background()
	t1, err := pdTimeCache.CurrentTimeFromPDCached(ctx)
	require.Nil(t, err)
	require.Equal(t, pdTimeCache.pdPhysicalTimeCache, t1)

	time.Sleep(10 * time.Millisecond)
	// should return cached time
	t2, err := pdTimeCache.CurrentTimeFromPDCached(ctx)
	require.Nil(t, err)
	require.Equal(t, pdTimeCache.pdPhysicalTimeCache, t2)
	require.Equal(t, t1, t2)

	time.Sleep(50 * time.Millisecond)
	// assume that the gc safe point updated one hour ago
	pdTimeCache.lastUpdatedPdTime = time.Now().Add(-time.Hour)
	t3, err := pdTimeCache.CurrentTimeFromPDCached(ctx)
	require.Nil(t, err)
	require.Equal(t, pdTimeCache.pdPhysicalTimeCache, t3)
	// should return new time
	require.NotEqual(t, t3, t2)
}
