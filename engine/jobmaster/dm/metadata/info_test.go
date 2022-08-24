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

package metadata

import (
	"context"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/pkg/meta/mock"
)

func TestInfoStore(t *testing.T) {
	ver1 := *semver.New("1.0.0")
	info := NewInfo(ver1)
	require.True(t, info.Version.Equal(ver1))

	infoStore := NewInfoStore("info_test", mock.NewMetaMock())
	key := infoStore.Key()
	keys, err := adapter.DMInfoKeyAdapter.Decode(key)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, keys[0], "info_test")

	state := infoStore.CreateState()
	require.IsType(t, &Info{}, state)

	state, err = infoStore.Get(context.Background())
	require.EqualError(t, err, "state not found")
	require.Nil(t, state)
	require.EqualError(t, infoStore.UpdateVersion(context.Background(), ver1), "state not found")
	require.NoError(t, infoStore.Put(context.Background(), info))
	state, err = infoStore.Get(context.Background())
	require.NoError(t, err)
	info2 := state.(*Info)
	require.True(t, info2.Version.Equal(info.Version))

	ver2 := *semver.New("2.0.0")
	infoStore.UpdateVersion(context.Background(), ver2)
	state, err = infoStore.Get(context.Background())
	require.NoError(t, err)
	info3 := state.(*Info)
	require.True(t, info3.Version.Equal(ver2))
}
