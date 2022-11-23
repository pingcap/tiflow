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

package bootstrap

import (
	"context"
	"sync"
	"testing"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestUpgrade(t *testing.T) {
	var (
		dummyUpgrader = NewDummyUpgrader()
		fromVer       = semver.New("6.1.0")
		ver           = "6.2.0"
	)

	dummyUpgrader.On("UpgradeFuncs").Return([]UpgradeFunc{}).Once()
	require.NoError(t, dummyUpgrader.Upgrade(context.Background(), *fromVer))

	dummyUpgrader.On("UpgradeFuncs").Return([]UpgradeFunc{
		{
			Version: *semver.New("6.1.0"),
			Upgrade: func(ctx context.Context) error {
				ver = "6.1.0"
				return nil
			},
		},
		{
			Version: *semver.New("6.3.0"),
			Upgrade: func(ctx context.Context) error {
				ver = "6.3.0"
				return nil
			},
		},
		{
			Version: *semver.New("6.2.1"),
			Upgrade: func(ctx context.Context) error {
				ver = "6.2.1"
				return nil
			},
		},
	}).Once()
	require.NoError(t, dummyUpgrader.Upgrade(context.Background(), *fromVer))
	require.Equal(t, "6.3.0", ver)

	dummyUpgrader.On("UpgradeFuncs").Return([]UpgradeFunc{
		{
			Version: *semver.New("6.3.0"),
			Upgrade: func(ctx context.Context) error {
				ver = "6.3.0"
				return errors.New("failed to upgrade to v6.3.0")
			},
		},
		{
			Version: *semver.New("6.2.1"),
			Upgrade: func(ctx context.Context) error {
				ver = "6.2.1"
				return nil
			},
		},
	}).Once()
	require.EqualError(t, dummyUpgrader.Upgrade(context.Background(), *fromVer), "failed to upgrade to v6.3.0")
	require.Equal(t, "6.3.0", ver)

	dummyUpgrader.On("UpgradeFuncs").Return([]UpgradeFunc{
		{
			Version: *semver.New("6.3.0"),
			Upgrade: func(ctx context.Context) error {
				ver = "6.3.0"
				return errors.New("failed to upgrade to v6.3.0")
			},
			Rollback: func(ctx context.Context) error {
				ver = "6.2.1"
				return errors.New("failed to rollback to v6.2.1")
			},
		},
		{
			Version: *semver.New("6.2.1"),
			Upgrade: func(ctx context.Context) error {
				ver = "6.2.1"
				return nil
			},
			Rollback: func(ctx context.Context) error {
				ver = "6.2.0"
				return errors.New("failed to rollback to v6.2.0")
			},
		},
	}).Once()
	require.EqualError(t, dummyUpgrader.Upgrade(context.Background(), *fromVer), "failed to upgrade to v6.3.0")
	require.Equal(t, "6.2.0", ver)
}

type DummyUpgrader struct {
	*DefaultUpgrader

	mock.Mock
	mu sync.Mutex
}

func NewDummyUpgrader() *DummyUpgrader {
	u := &DummyUpgrader{
		DefaultUpgrader: NewDefaultUpgrader(log.L()),
	}
	u.DefaultUpgrader.Upgrader = u
	return u
}

func (upgrader *DummyUpgrader) UpgradeFuncs() []UpgradeFunc {
	upgrader.mu.Lock()
	defer upgrader.mu.Unlock()
	args := upgrader.Called()
	return args.Get(0).([]UpgradeFunc)
}
