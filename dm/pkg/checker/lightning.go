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

package checker

import (
	"context"

	"github.com/pingcap/tidb/br/pkg/lightning/mydump"
	"github.com/pingcap/tidb/br/pkg/lightning/restore"
)

// LightningEmptyRegionChecker checks whether there are too many empty regions in the cluster.
type LightningEmptyRegionChecker struct {
	inner restore.PrecheckItem
}

func NewLightningEmptyRegionChecker(
	preInfoGetter restore.PreRestoreInfoGetter,
	dbMetas []*mydump.MDDatabaseMeta,
) RealChecker {
	return &LightningEmptyRegionChecker{inner: restore.NewEmptyRegionCheckItem(preInfoGetter, dbMetas)}
}

// Name implements the RealChecker interface.
func (c *LightningEmptyRegionChecker) Name() string {
	return "lightning_empty_region"
}

// Check implements the RealChecker interface.
func (c *LightningEmptyRegionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check downstream cluster has too many empty regions",
		State: StateFailure,
	}
	lightningResult, err := c.inner.Check(ctx)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	if lightningResult.Message != "" {
		result.State = StateWarning
		result.Errors = append(result.Errors, &Error{Severity: StateWarning, ShortErr: lightningResult.Message})
		return result
	}
	result.State = StateSuccess
	return result
}
