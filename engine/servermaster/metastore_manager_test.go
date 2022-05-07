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

package servermaster

import (
	"testing"

	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/stretchr/testify/require"
)

func TestMetaStoreManager(t *testing.T) {
	t.Parallel()

	storeConf := &metaclient.StoreConfigParams{
		Endpoints: []string{
			"127.0.0.1",
			"127.0.0.2",
		},
	}

	manager := NewMetaStoreManager()
	err := manager.Register("root", storeConf)
	require.Nil(t, err)

	err = manager.Register("root", &metaclient.StoreConfigParams{})
	require.Error(t, err)

	store := manager.GetMetaStore("default")
	require.Nil(t, store)

	store = manager.GetMetaStore("root")
	require.NotNil(t, store)
	require.Equal(t, storeConf, store)

	manager.UnRegister("root")
	store = manager.GetMetaStore("root")
	require.Nil(t, store)
}

func TestDefaultMetaStoreManager(t *testing.T) {
	t.Parallel()

	store := NewFrameMetaConfig()
	require.Equal(t, metaclient.FrameMetaID, store.StoreID)
	require.Equal(t, pkgOrm.DefaultFrameMetaEndpoints, store.Endpoints[0])

	store = NewDefaultUserMetaConfig()
	require.Equal(t, metaclient.DefaultUserMetaID, store.StoreID)
	require.Equal(t, metaclient.DefaultUserMetaEndpoints, store.Endpoints[0])
}
