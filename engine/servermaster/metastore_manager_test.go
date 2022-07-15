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

	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestMetaStoreManager(t *testing.T) {
	t.Parallel()

	storeConf := &metaModel.StoreConfig{
		Endpoints: []string{
			"127.0.0.1",
			"127.0.0.2",
		},
	}

	manager := NewMetaStoreManager()
	err := manager.Register("root", storeConf)
	require.Nil(t, err)

	err = manager.Register("root", &metaModel.StoreConfig{})
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
