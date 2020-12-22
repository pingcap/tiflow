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

package util

import (
	"context"
	"math"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
)

type gcServiceSuite struct {
	pdCli mockPdClientForServiceGCSafePoint
}

var _ = check.Suite(&gcServiceSuite{
	mockPdClientForServiceGCSafePoint{serviceSafePoint: make(map[string]uint64)},
})

func (s *gcServiceSuite) TestCheckSafetyOfStartTs(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service1", 10, 60) //nolint:errcheck
	err := CheckSafetyOfStartTs(ctx, s.pdCli, 50)
	c.Assert(err.Error(), check.Equals, "startTs less than gcSafePoint: [tikv:9006]GC life time is shorter than transaction duration, transaction starts at 50, GC safe point is 60")
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service2", 10, 80) //nolint:errcheck
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service3", 10, 70) //nolint:errcheck
	err = CheckSafetyOfStartTs(ctx, s.pdCli, 65)
	c.Assert(err, check.IsNil)
	c.Assert(s.pdCli.serviceSafePoint, check.DeepEquals, map[string]uint64{"service1": 60, "service2": 80, "service3": 70, "ticdc-changefeed-creating": 65})

}

type mockPdClientForServiceGCSafePoint struct {
	pd.Client
	serviceSafePoint map[string]uint64
}

func (m mockPdClientForServiceGCSafePoint) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	minSafePoint := uint64(math.MaxUint64)
	for _, safePoint := range m.serviceSafePoint {
		if minSafePoint > safePoint {
			minSafePoint = safePoint
		}
	}
	if safePoint < minSafePoint && len(m.serviceSafePoint) != 0 {
		return minSafePoint, nil
	}
	m.serviceSafePoint[serviceID] = safePoint
	return minSafePoint, nil
}
