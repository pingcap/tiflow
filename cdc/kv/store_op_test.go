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

package kv

import (
	"sync"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestStorage(t *testing.T) {
	tiStore, err := mockstore.NewMockStore(mockstore.WithStoreType(mockstore.MockTiKV))
	require.Nil(t, err)
	log.Info(tiStore.UUID())
	store := wrapStore(tiStore)
	wrapCount := 100
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for i := 0; i < wrapCount; i++ {
			temp := wrapStore(tiStore)
			require.Equal(t, store, temp)
		}
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		for i := 0; i < wrapCount+1; i++ {
			err := store.Close()
			require.Nil(t, err)
		}
		wg.Done()
	}()
	wg.Wait()
	mc.Mutex.Lock()
	defer mc.Mutex.Unlock()
	require.Len(t, mc.cache, 0)
}
