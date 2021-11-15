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

package kv

import (
	"testing"

	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/tikv/client-go/v2/testutils"
)

func TestRegionCacheSingleton(t *testing.T) {
	const N = 10000
	_, _, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	if err != nil {
		t.Error(err)
	}

	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	rc := getRegionCacheInstance(pdClient)
	for i := 0; i < N; i++ {
		v := getRegionCacheInstance(pdClient)
		if v != rc {
			t.Fatalf("regioncache singleton is not the same")
		}
	}
}

func BenchmarkRegionCacheSingleton(t *testing.B) {
	_, _, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	if err != nil {
		t.Error(err)
	}

	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	for i := 0; i < t.N; i++ {
		getRegionCacheInstance(pdClient)
	}
}
