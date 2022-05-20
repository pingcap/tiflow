// Copyright 2022 PingCAP, Inc.
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

package upstream

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/stretchr/testify/require"
)

func TestUpstream(t *testing.T) {
	pdClient := &gc.MockPDClient{}
	manager := NewManager4Test(pdClient)

	up1 := manager.Get(testConfig)
	require.NotNil(t, up1)

	// test Add
	conf := Config{
		ID:          1,
		PDEndpoints: "",
		KeyPath:     "",
		CertPath:    "",
		CAPath:      "",
	}
	manager.add(conf)

	// test Get
	require.NotNil(t, manager.Get(conf))
	up2 := NewUpstream4Test(pdClient)
	up2.ID = conf.ID
	mockClock := clock.NewMock()
	up2.clock = mockClock
	require.NotNil(t, manager.Get(testConfig))

	// test Tick
	up2.Release()
	mockClock.Add(maxIdleDuration * 2)

	manager.Tick(context.Background())
	require.NotNil(t, manager.Get(testConfig))
	require.NotNil(t, manager.Get(conf))
}
