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

package pdutil

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
	t.Parallel()
	mockPDClient := &MockPDClient{}
	clock, err := NewClock(context.Background(), mockPDClient)
	require.NoError(t, err)

	go clock.Run(context.Background())
	defer clock.Stop()
	time.Sleep(1 * time.Second)

	t1, err := clock.CurrentTime()
	require.Nil(t, err)

	time.Sleep(400 * time.Millisecond)
	// assume that the gc safe point updated one hour ago
	t2, err := clock.CurrentTime()
	require.Nil(t, err)
	// should return new time
	require.NotEqual(t, t1, t2)
}
