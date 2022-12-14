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

package gc

import (
	"context"
	"math"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	pd "github.com/tikv/pd/client"
)

type gcServiceSuite struct {
	pdCli *mockPdClientForServiceGCSafePoint
}

var _ = check.Suite(&gcServiceSuite{
	&mockPdClientForServiceGCSafePoint{serviceSafePoint: make(map[string]uint64)},
})

func (s *gcServiceSuite) TestCheckSafetyOfStartTs(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()

	TTL := int64(1)
	// assume no pd leader switch
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service1", 10, 60) //nolint:errcheck
	err := EnsureChangefeedStartTsSafety(ctx, s.pdCli, "changefeed1", TTL, 50)
	c.Assert(err.Error(), check.Equals, "[CDC:ErrStartTsBeforeGC]fail to create changefeed because start-ts 50 is earlier than GC safepoint at 60")
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service2", 10, 80) //nolint:errcheck
	s.pdCli.UpdateServiceGCSafePoint(ctx, "service3", 10, 70) //nolint:errcheck
	err = EnsureChangefeedStartTsSafety(ctx, s.pdCli, "changefeed2", TTL, 65)
	c.Assert(err, check.IsNil)
	c.Assert(s.pdCli.serviceSafePoint, check.DeepEquals, map[string]uint64{
		"service1":                   60,
		"service2":                   80,
		"service3":                   70,
		"ticdc-creating-changefeed2": 65,
	})

	s.pdCli.enableLeaderSwitch = true

	s.pdCli.retryThreshold = 1
	s.pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, s.pdCli, "changefeed2", TTL, 65)
	c.Assert(err, check.IsNil)

	s.pdCli.retryThreshold = gcServiceMaxRetries + 1
	s.pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, s.pdCli, "changefeed2", TTL, 65)
	c.Assert(err, check.NotNil)
	c.Assert(err.Error(), check.Equals, "[CDC:ErrReachMaxTry]reach maximum try: 9")

	s.pdCli.retryThreshold = 3
	s.pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, s.pdCli, "changefeed1", TTL, 50)
	c.Assert(err.Error(), check.Equals, "[CDC:ErrStartTsBeforeGC]fail to create changefeed because start-ts 50 is earlier than GC safepoint at 60")
}

type mockPdClientForServiceGCSafePoint struct {
	pd.Client
	serviceSafePoint   map[string]uint64
	enableLeaderSwitch bool
	retryCount         int
	retryThreshold     int
}

func (m *mockPdClientForServiceGCSafePoint) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	defer func() { m.retryCount++ }()
	minSafePoint := uint64(math.MaxUint64)
	if m.enableLeaderSwitch && m.retryCount < m.retryThreshold {
		// simulate pd leader switch error
		return minSafePoint, errors.New("not pd leader")
	}

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
