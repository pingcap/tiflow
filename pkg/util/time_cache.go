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
	"time"

	"github.com/pingcap/errors"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

const pdTimeUpdateInterval = 1 * time.Second

type PDTimeCache struct {
	pdClient            pd.Client
	pdPhysicalTimeCache time.Time
	lastUpdatedPdTime   time.Time
}

func NewPDTimeCache(pdClient pd.Client) *PDTimeCache {
	return &PDTimeCache{
		pdClient: pdClient,
	}
}

func (m *PDTimeCache) CurrentTimeFromPDCached(ctx context.Context) (time.Time, error) {
	if time.Since(m.lastUpdatedPdTime) <= pdTimeUpdateInterval {
		return m.pdPhysicalTimeCache, nil
	}
	physical, _, err := m.pdClient.GetTS(ctx)
	if err != nil {
		return time.Now(), errors.Trace(err)
	}
	m.pdPhysicalTimeCache = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
	m.lastUpdatedPdTime = time.Now()
	return m.pdPhysicalTimeCache, nil
}
