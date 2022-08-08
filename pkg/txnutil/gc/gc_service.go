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

package gc

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const (
	// EnsureGCServiceCreating is a tag of GC service id for changefeed creation
	EnsureGCServiceCreating = "-creating-"
	// EnsureGCServiceResuming is a tag of GC service id for changefeed resumption
	EnsureGCServiceResuming = "-resuming-"
	// EnsureGCServiceInitializing is a tag of GC service id for changefeed initialization
	EnsureGCServiceInitializing = "-initializing-"
)

// EnsureChangefeedStartTsSafety checks if the startTs less than the minimum of
// service GC safepoint and this function will update the service GC to startTs
func EnsureChangefeedStartTsSafety(
	ctx context.Context, pdCli pd.Client,
	gcServiceIDPrefix string,
	changefeedID model.ChangeFeedID,
	TTL int64, startTs uint64,
) error {
	minServiceGCTs, err := SetServiceGCSafepoint(
		ctx, pdCli,
		gcServiceIDPrefix+changefeedID.Namespace+"_"+changefeedID.ID,
		TTL, startTs)
	if err != nil {
		return errors.Trace(err)
	}
	// startTs should be greater than or equal to minServiceGCTs + 1, otherwise gcManager
	// would return a ErrSnapshotLostByGC even though the changefeed would appear to be successfully
	// created/resumed. See issue #6350 for more detail.
	if startTs > 0 && startTs < minServiceGCTs+1 {
		return cerrors.ErrStartTsBeforeGC.GenWithStackByArgs(startTs, minServiceGCTs)
	}
	return nil
}

// UndoEnsureChangefeedStartTsSafety cleans the service GC safepoint of a changefeed
// if something goes wrong after successfully calling EnsureChangefeedStartTsSafety().
func UndoEnsureChangefeedStartTsSafety(
	ctx context.Context, pdCli pd.Client,
	gcServiceIDPrefix string,
	changefeedID model.ChangeFeedID,
) error {
	err := RemoveServiceGCSafepoint(
		ctx,
		pdCli,
		gcServiceIDPrefix+changefeedID.Namespace+"_"+changefeedID.ID)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// PD leader switch may happen, so just gcServiceMaxRetries it.
// The default PD election timeout is 3 seconds. Triple the timeout as
// retry time to make sure PD leader can be elected during retry.
const (
	gcServiceBackoffDelay = 1000 // 1s
	gcServiceMaxRetries   = 9
)

// SetServiceGCSafepoint set a service safepoint to PD.
func SetServiceGCSafepoint(
	ctx context.Context, pdCli pd.Client, serviceID string, TTL int64, safePoint uint64,
) (minServiceGCTs uint64, err error) {
	err = retry.Do(ctx,
		func() error {
			var err1 error
			minServiceGCTs, err1 = pdCli.UpdateServiceGCSafePoint(ctx, serviceID, TTL, safePoint)
			if err1 != nil {
				log.Warn("Set GC safepoint failed, retry later", zap.Error(err1))
			}
			return err1
		},
		retry.WithBackoffBaseDelay(gcServiceBackoffDelay),
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
	return
}

// RemoveServiceGCSafepoint removes a service safepoint from PD.
func RemoveServiceGCSafepoint(ctx context.Context, pdCli pd.Client, serviceID string) error {
	// Set TTL to 0 second to delete the service safe point.
	TTL := 0
	return retry.Do(ctx,
		func() error {
			_, err := pdCli.UpdateServiceGCSafePoint(ctx, serviceID, int64(TTL), math.MaxUint64)
			if err != nil {
				log.Warn("Remove GC safepoint failed, retry later", zap.Error(err))
			}
			return err
		},
		retry.WithBackoffBaseDelay(gcServiceBackoffDelay), // 1s
		retry.WithMaxTries(gcServiceMaxRetries),
		retry.WithIsRetryableErr(cerrors.IsRetryableError))
}
