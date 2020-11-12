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
	"github.com/pingcap/tidb/store/tikv"
	pd "github.com/tikv/pd/client"
)

const (
	// CDCChangefeedCreatingServiceGCSafePointID is service GC safe point ID
	CDCChangefeedCreatingServiceGCSafePointID = "ticdc-changefeed-creating"
	// CDCChangefeedCreatingServiceGCSafePointTTL is service GC safe point TTL
	CDCChangefeedCreatingServiceGCSafePointTTL = 10 * 60 // 10 mins
)

// CheckSafetyOfStartTs checks if the startTs less than the minimum of Service-GC-Ts
// and this function will update the service GC to startTs
func CheckSafetyOfStartTs(ctx context.Context, pdCli pd.Client, startTs uint64) error {
	minServiceGCTs, err := pdCli.UpdateServiceGCSafePoint(ctx, CDCChangefeedCreatingServiceGCSafePointID,
		CDCChangefeedCreatingServiceGCSafePointTTL, startTs)
	if err != nil {
		return errors.Trace(err)
	}
	if startTs < minServiceGCTs {
		return errors.Wrap(tikv.ErrGCTooEarly.GenWithStackByArgs(startTs, minServiceGCTs), "startTs less than gcSafePoint")
	}
	return nil
}
