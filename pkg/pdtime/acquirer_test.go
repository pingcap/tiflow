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

package pdtime

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/tiflow/pkg/util/testleak"

	"github.com/pingcap/tidb/store/tikv/oracle"
	pd "github.com/tikv/pd/client"
)

func TestSuite(t *testing.T) { check.TestingT(t) }

type pdTimeSuite struct{}

var _ = check.Suite(&pdTimeSuite{})

// MockPDClient mocks pd.Client to facilitate unit testing.
type MockPDClient struct {
	pd.Client
}

// GetTS implements pd.Client.GetTS.
func (m *MockPDClient) GetTS(ctx context.Context) (int64, int64, error) {
	return oracle.GetPhysical(time.Now()), 0, nil
}

func (s *pdTimeSuite) TestTimeFromPD(c *check.C) {
	defer testleak.AfterTest(c)()
	mockPDClient := &MockPDClient{}
	TimeAcquirer := NewTimeAcquirer(mockPDClient)
	go TimeAcquirer.Run(context.Background())
	defer TimeAcquirer.Stop()
	time.Sleep(1 * time.Second)

	t1, err := TimeAcquirer.CurrentTimeFromCached()
	c.Assert(err, check.IsNil)

	time.Sleep(400 * time.Millisecond)
	// assume that the gc safe point updated one hour ago
	t2, err := TimeAcquirer.CurrentTimeFromCached()
	c.Assert(err, check.IsNil)
	// should return new time
	c.Assert(t1, check.Less, t2)
}
