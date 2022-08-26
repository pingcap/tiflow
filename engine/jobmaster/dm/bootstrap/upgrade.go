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
	"sort"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
)

// UpgradeFunc defines the function for upgrade.
type UpgradeFunc struct {
	Version  semver.Version
	Upgrade  func(ctx context.Context) error
	Rollback func(ctx context.Context) error
}

// Upgrader represents the upgrader interface.
type Upgrader interface {
	UpgradeFuncs() []UpgradeFunc
}

// DefaultUpgrader defines the default Upgrade steps.
type DefaultUpgrader struct {
	Upgrader

	logger *zap.Logger
}

// NewDefaultUpgrader returns a new DefaultUpgrader.
func NewDefaultUpgrader(pLogger *zap.Logger) *DefaultUpgrader {
	return &DefaultUpgrader{
		logger: pLogger.With(zap.String("component", "upgrader")),
	}
}

// Upgrade run the upgrade function in order.
// if any upgrade failed, try to rollback all of them.
func (upgrader *DefaultUpgrader) Upgrade(ctx context.Context, fromVer semver.Version) error {
	var (
		err           error
		rollbackFuncs = make([]func(ctx context.Context) error, 0)
	)
	for _, upgradeFunc := range upgrader.upgradeFuncs() {
		if upgradeFunc.Version.Compare(fromVer) <= 0 {
			continue
		}
		upgrader.logger.Info("start upgrading", zap.Stringer("internal_version", upgradeFunc.Version))
		rollbackFuncs = append(rollbackFuncs, upgradeFunc.Rollback)
		if err = upgradeFunc.Upgrade(ctx); err != nil {
			upgrader.logger.Error("upgrade failed", zap.Error(err))
			break
		}
	}
	if err != nil {
		for i := len(rollbackFuncs) - 1; i >= 0; i-- {
			rollback := rollbackFuncs[i]
			if rollback != nil {
				if err2 := rollback(ctx); err2 != nil {
					upgrader.logger.Error("rollback failed", zap.Error(err2))
					continue
				}
			}
		}
	}
	return err
}

// upgradeFuncs sort the upgrade functions for Upgrader.
// NOTE: though we sort the upgrade functions, the upgrade order should still be linear.
// e.g. If we release in such order: v6.1.0, v6.2.0, v6.1.1
// the upgrade order should be: v6.1.0, v6.1.1, v6.2.0
// but it's impossible to do that since v6.2.0 was released before v6.1.1
// That is to say, we should not upgrade patch version after we upgrade minor version, as well as minor and major.
func (upgrader *DefaultUpgrader) upgradeFuncs() []UpgradeFunc {
	funcs := upgrader.UpgradeFuncs()
	sort.Slice(funcs, func(i, j int) bool {
		return funcs[i].Version.LessThan(funcs[j].Version)
	})
	return funcs
}
