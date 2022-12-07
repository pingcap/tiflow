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
	"fmt"
	"strings"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/lightning/restore"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

func convertLightningPrecheck(
	ctx context.Context,
	dmResult *Result,
	lightningPrechecker restore.PrecheckItem,
	failLevel State,
	instruction string,
) {
	lightningResult, err := lightningPrechecker.Check(ctx)
	if err != nil {
		markCheckError(dmResult, err)
		return
	}
	if !lightningResult.Passed {
		dmResult.State = failLevel
		dmResult.Instruction = instruction
		dmResult.Errors = append(dmResult.Errors, &Error{Severity: failLevel, ShortErr: lightningResult.Message})
		return
	}
	dmResult.State = StateSuccess
}

// LightningEmptyRegionChecker checks whether there are too many empty regions in the cluster.
type LightningEmptyRegionChecker struct {
	inner restore.PrecheckItem
}

// NewLightningEmptyRegionChecker creates a new LightningEmptyRegionChecker.
func NewLightningEmptyRegionChecker(lightningChecker restore.PrecheckItem) RealChecker {
	return &LightningEmptyRegionChecker{inner: lightningChecker}
}

// Name implements the RealChecker interface.
func (c *LightningEmptyRegionChecker) Name() string {
	return "lightning_empty_region"
}

// Check implements the RealChecker interface.
func (c *LightningEmptyRegionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check whether there are too many empty Regions in the TiKV under physical import mode",
		State: StateFailure,
	}
	convertLightningPrecheck(
		ctx,
		result,
		c.inner,
		StateWarning,
		`you can change "region merge" related configuration in PD to speed up eliminating empty regions`,
	)
	return result
}

// LightningRegionDistributionChecker checks whether the region distribution is balanced.
type LightningRegionDistributionChecker struct {
	inner restore.PrecheckItem
}

// NewLightningRegionDistributionChecker creates a new LightningRegionDistributionChecker.
func NewLightningRegionDistributionChecker(lightningChecker restore.PrecheckItem) RealChecker {
	return &LightningRegionDistributionChecker{inner: lightningChecker}
}

// Name implements the RealChecker interface.
func (c *LightningRegionDistributionChecker) Name() string {
	return "lightning_region_distribution"
}

// Check implements the RealChecker interface.
func (c *LightningRegionDistributionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check whether the Regions in the TiKV cluster are distributed evenly under physical import mode",
		State: StateFailure,
	}
	convertLightningPrecheck(
		ctx,
		result,
		c.inner,
		StateWarning,
		`you can change "region schedule" related configuration in PD to speed up balancing regions`,
	)
	return result
}

// LightningClusterVersionChecker checks whether the cluster version is compatible with Lightning.
type LightningClusterVersionChecker struct {
	inner restore.PrecheckItem
}

// NewLightningClusterVersionChecker creates a new LightningClusterVersionChecker.
func NewLightningClusterVersionChecker(lightningChecker restore.PrecheckItem) RealChecker {
	return &LightningClusterVersionChecker{inner: lightningChecker}
}

// Name implements the RealChecker interface.
func (c *LightningClusterVersionChecker) Name() string {
	return "lightning_cluster_version"
}

// Check implements the RealChecker interface.
func (c *LightningClusterVersionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check whether the downstream TiDB/PD/TiKV version meets the requirements of physical import mode",
		State: StateFailure,
	}
	convertLightningPrecheck(
		ctx,
		result,
		c.inner,
		StateFailure,
		`you can switch to logical import mode which has no requirements on downstream cluster version`,
	)
	return result
}

// LightningFreeSpaceChecker checks whether the cluster has enough free space.
type LightningFreeSpaceChecker struct {
	sourceDataSize int64
	infoGetter     restore.TargetInfoGetter
}

// NewLightningFreeSpaceChecker creates a new LightningFreeSpaceChecker.
func NewLightningFreeSpaceChecker(sourceDataSize int64, getter restore.TargetInfoGetter) RealChecker {
	return &LightningFreeSpaceChecker{
		sourceDataSize: sourceDataSize,
		infoGetter:     getter,
	}
}

// Name implements the RealChecker interface.
func (c *LightningFreeSpaceChecker) Name() string {
	return "lightning_free_space"
}

// Check implements the RealChecker interface.
func (c *LightningFreeSpaceChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check whether the downstream has enough free space to store the data to be migrated",
		State: StateFailure,
	}
	storeInfo, err := c.infoGetter.GetStorageInfo(ctx)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	clusterAvail := uint64(0)
	for _, store := range storeInfo.Stores {
		clusterAvail += uint64(store.Status.Available)
	}
	if clusterAvail < uint64(c.sourceDataSize) {
		result.State = StateFailure
		result.Errors = append(result.Errors, &Error{
			Severity: StateFailure,
			ShortErr: fmt.Sprintf("Downstream doesn't have enough space, available is %s, but we need %s",
				units.BytesSize(float64(clusterAvail)), units.BytesSize(float64(c.sourceDataSize))),
		})
		result.Instruction = "you can try to scale-out TiKV storage or TiKV instance to gain more storage space"
		return result
	}

	replConfig, err := c.infoGetter.GetReplicationConfig(ctx)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	safeSize := uint64(c.sourceDataSize) * replConfig.MaxReplicas * 2
	if clusterAvail < safeSize {
		result.State = StateWarning
		result.Errors = append(result.Errors, &Error{
			Severity: StateWarning,
			ShortErr: fmt.Sprintf("Cluster may not have enough space, available is %s, but we need %s",
				units.BytesSize(float64(clusterAvail)), units.BytesSize(float64(safeSize))),
		})
		result.Instruction = "you can try to scale-out TiKV storage or TiKV instance to gain more storage space"
		return result
	}
	result.State = StateSuccess
	return result
}

// LightningSortingSpaceChecker checks the local disk has enough space for physical
// import mode sorting data.
type LightningSortingSpaceChecker struct {
	inner restore.PrecheckItem
}

// NewLightningSortingSpaceChecker creates a new LightningSortingSpaceChecker.
func NewLightningSortingSpaceChecker(lightningChecker restore.PrecheckItem) RealChecker {
	return &LightningSortingSpaceChecker{inner: lightningChecker}
}

// Name implements the RealChecker interface.
func (c *LightningSortingSpaceChecker) Name() string {
	return "lightning_enough_sorting_space"
}

// Check implements the RealChecker interface.
func (c *LightningSortingSpaceChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check whether the free space of sorting-dir-physical is enough for physical import mode",
		State: StateFailure,
	}
	convertLightningPrecheck(
		ctx,
		result,
		c.inner,
		StateWarning,
		`you can change sorting-dir-physical to another mounting disk or set disk-quota-physical`,
	)
	// don't expose lightning's config name in message
	if len(result.Errors) > 0 {
		result.Errors[0].ShortErr = strings.ReplaceAll(result.Errors[0].ShortErr, "tikv-importer.disk-quota", "disk-quota-physical")
		result.Errors[0].ShortErr = strings.ReplaceAll(result.Errors[0].ShortErr, "mydumper.sorted-kv-dir", "sorting-dir-physical")
	}
	return result
}

// LightningCDCPiTRChecker checks whether the cluster has running CDC PiTR tasks.
type LightningCDCPiTRChecker struct {
	inner restore.PrecheckItem
}

// NewLightningCDCPiTRChecker creates a new LightningClusterVersionChecker.
func NewLightningCDCPiTRChecker(lightningChecker restore.PrecheckItem) RealChecker {
	c, ok := lightningChecker.(*restore.CDCPITRCheckItem)
	if ok {
		c.Instruction = "physical import mode is not compatible with them. Please switch to logical import mode then try again."
	} else {
		log.L().DPanic("lightningChecker is not CDCPITRCheckItem")
	}
	return &LightningCDCPiTRChecker{inner: lightningChecker}
}

// Name implements the RealChecker interface.
func (c *LightningCDCPiTRChecker) Name() string {
	return "lightning_downstream_cdc_pitr"
}

// Check implements the RealChecker interface.
func (c *LightningCDCPiTRChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  c.Name(),
		Desc:  "check whether the downstream has tasks incompatible with physical import mode",
		State: StateFailure,
	}
	convertLightningPrecheck(
		ctx,
		result,
		c.inner,
		StateFailure,
		`you can switch to logical import mode which has no requirements on this`,
	)
	return result
}
