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

package mock

import (
	"context"
	"math"

	"github.com/stretchr/testify/mock"
	pd "github.com/tikv/pd/client"
)

type MockPDClient struct {
	pd.Client
	mock.Mock

	simulateGCSafePoint bool

	serviceSafePoints map[string]uint64
}

func NewMockPDClient(initGCSafePoint uint64) *MockPDClient {
	return &MockPDClient{
		simulateGCSafePoint: true,
		serviceSafePoints: map[string]uint64{
			"gcworker": initGCSafePoint,
		},
	}
}

func (c *MockPDClient) ClearGCSafePoint() {
	c.serviceSafePoints = make(map[string]uint64)
}

func (c *MockPDClient) UpdateServiceGCSafePoint(ctx context.Context, serviceID string, ttl int64, safePoint uint64) (uint64, error) {
	if c.simulateGCSafePoint {
		minSafePoint := uint64(math.MaxUint64)
		for _, ts := range c.serviceSafePoints {
			if ts < minSafePoint {
				minSafePoint = ts
			}
		}

		if safePoint < minSafePoint {
			return minSafePoint, nil
		}

		if ttl <= 0 {
			delete(c.serviceSafePoints, serviceID)
		} else {
			c.serviceSafePoints[serviceID] = safePoint
		}

		minSafePoint = uint64(math.MaxUint64)
		for _, ts := range c.serviceSafePoints {
			if ts < minSafePoint {
				minSafePoint = ts
			}
		}

		return minSafePoint, nil
	}

	args := c.Called(ctx, serviceID, ttl, safePoint)
	return args.Get(0).(uint64), args.Error(1)
}
