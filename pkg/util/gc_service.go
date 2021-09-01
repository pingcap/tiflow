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

package util

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	// cdcChangefeedCreatingServiceGCSafePointID is service GC safe point ID
	cdcChangefeedCreatingServiceGCSafePointID = "ticdc-creating-"
	// cdcChangefeedCreatingServiceGCSafePointTTL is service GC safe point TTL
	cdcChangefeedCreatingServiceGCSafePointTTL = 60 * 60 // 60 mins
)

// CheckSafetyOfStartTs checks if the startTs less than the minimum of Service-GC-Ts
// and this function will update the service GC to startTs
func CheckSafetyOfStartTs(
	ctx context.Context, pdCli pd.Client, changefeedID string, startTs uint64,
) error {
	minServiceGCTs, err := SetServiceGCSafepoint(
		ctx, pdCli, cdcChangefeedCreatingServiceGCSafePointID+changefeedID,
		cdcChangefeedCreatingServiceGCSafePointTTL, startTs)
	if err != nil {
		return errors.Trace(err)
	}
	if startTs < minServiceGCTs {
		return cerrors.ErrStartTsBeforeGC.GenWithStackByArgs(startTs, minServiceGCTs)
	}
	return nil
}

// PD leader switch may happen, so just serviceGCSafepointRetry it.
// The default PD election timeout is 3 seconds. Triple the timeout as
// retry time to make sure PD leader can be elected during retry.
const (
	serviceGCSafepointBackoffDelay = 1000 // 1s
	serviceGCSafepointRetry        = 9
)

// SetServiceGCSafepoint set a service safepoint to PD.
func SetServiceGCSafepoint(
	ctx context.Context, pdCli pd.Client, serviceID string, TTL int64, safePoint uint64,
) (minServiceGCTs uint64, err error) {
	retry.Do(ctx,
		func() error {
			minServiceGCTs, err = pdCli.UpdateServiceGCSafePoint(ctx, serviceID, TTL, safePoint)
			if err != nil {
				log.Warn("Set GC safepoint failed, retry later", zap.Error(err))
			}
			return err
		},
		retry.WithBackoffBaseDelay(serviceGCSafepointBackoffDelay),
		retry.WithMaxTries(serviceGCSafepointRetry),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
	return
}

// RemoveServiceGCSafepoint removes a service safepoint from PD.
func RemoveServiceGCSafepoint(ctx context.Context, pdCli pd.Client, serviceID string) error {
	// Set TTL to 1 second to delete the service safe point effectively.
	TTL := 1
	return retry.Do(ctx,
		func() error {
			_, err := pdCli.UpdateServiceGCSafePoint(ctx, serviceID, int64(TTL), math.MaxUint64)
			if err != nil {
				log.Warn("Remove GC safepoint failed, retry later", zap.Error(err))
			}
			return err
		},
		retry.WithBackoffBaseDelay(serviceGCSafepointBackoffDelay), // 1s
		retry.WithMaxTries(serviceGCSafepointRetry),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
}
