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
	cdcChangefeedCreatingServiceGCSafePointTTL = 10 * 60 // 10 mins
)

// CheckSafetyOfStartTs checks if the startTs less than the minimum of Service-GC-Ts
// and this function will update the service GC to startTs
func CheckSafetyOfStartTs(ctx context.Context, pdCli pd.Client, changefeedID string, startTs uint64) (err error) {
	var minServiceGCTs uint64
	// pd leader switch may happen, so just retry it.
	if err := retry.Do(ctx, func() error {
		minServiceGCTs, err = pdCli.UpdateServiceGCSafePoint(ctx, cdcChangefeedCreatingServiceGCSafePointID+changefeedID,
			cdcChangefeedCreatingServiceGCSafePointTTL, startTs)
		if err != nil {
			log.Warn("update GC safepoint failed, retry later", zap.Error(err))
		}
		return err
	},
		retry.WithBackoffBaseDelay(500),
		retry.WithBackoffMaxDelay(60*1000),
		retry.WithMaxTries(8),
		retry.WithIsRetryableErr(cerrors.IsRetryableError)); err != nil {
		return errors.Trace(err)
	}
	if startTs < minServiceGCTs {
		return cerrors.ErrStartTsBeforeGC.GenWithStackByArgs(startTs, minServiceGCTs)
	}
	return nil
}
