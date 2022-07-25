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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestCheckSafetyOfStartTs(t *testing.T) {
	t.Parallel()

	pdCli := &mockPdClientForServiceGCSafePoint{serviceSafePoint: make(map[string]uint64)}

	ctx := context.Background()

	TTL := int64(1)
	// assume no pd leader switch
	pdCli.UpdateServiceGCSafePoint(ctx, "service1", 10, 60) //nolint:errcheck
	err := EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		model.DefaultChangeFeedID("changefeed1"), TTL, 50)
	require.Equal(t,
		"[CDC:ErrStartTsBeforeGC]fail to create or maintain changefeed "+
			"because start-ts 50 is earlier than or equal to GC safepoint at 60", err.Error())
	pdCli.UpdateServiceGCSafePoint(ctx, "service2", 10, 80) //nolint:errcheck
	pdCli.UpdateServiceGCSafePoint(ctx, "service3", 10, 70) //nolint:errcheck
	err = EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		model.DefaultChangeFeedID("changefeed2"), TTL, 65)
	require.Nil(t, err)
	require.Equal(t, pdCli.serviceSafePoint, map[string]uint64{
		"service1":                           60,
		"service2":                           80,
		"service3":                           70,
		"ticdc-creating-default_changefeed2": 65,
	})
	err = UndoEnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		model.DefaultChangeFeedID("changefeed2"))
	require.Nil(t, err)
	require.Equal(t, pdCli.serviceSafePoint, map[string]uint64{
		"service1":                           60,
		"service2":                           80,
		"service3":                           70,
		"ticdc-creating-default_changefeed2": math.MaxUint64,
	})

	pdCli.enableLeaderSwitch = true

	pdCli.retryThreshold = 1
	pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		model.DefaultChangeFeedID("changefeed2"), TTL, 65)
	require.Nil(t, err)

	pdCli.retryThreshold = gcServiceMaxRetries + 1
	pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		model.DefaultChangeFeedID("changefeed2"), TTL, 65)
	require.NotNil(t, err)
	require.Equal(t, err.Error(),
		"[CDC:ErrReachMaxTry]reach maximum try: 9, error: not pd leader: not pd leader")

	pdCli.retryThreshold = 3
	pdCli.retryCount = 0
	err = EnsureChangefeedStartTsSafety(ctx, pdCli,
		"ticdc-creating-",
		model.DefaultChangeFeedID("changefeed1"), TTL, 50)
	require.Equal(t, err.Error(),
		"[CDC:ErrStartTsBeforeGC]fail to create or maintain changefeed "+
			"because start-ts 50 is earlier than or equal to GC safepoint at 60")
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
