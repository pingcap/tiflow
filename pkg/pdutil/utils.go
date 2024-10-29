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

package pdutil

import (
	"context"
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const sourceIDName = "source_id"

// GetSourceID returns the source ID of the TiDB cluster that PD is belonged to.
func GetSourceID(ctx context.Context, pdClient pd.Client) (uint64, error) {
	// only nil in test case
	if pdClient == nil {
		return config.DefaultTiDBSourceID, nil
	}
	// The default value of sourceID is 1,
	// which means the sourceID is not changed by user.
	sourceID := uint64(1)
	sourceIDConfig, _, err := pdClient.LoadGlobalConfig(ctx, []string{sourceIDName}, "")
	if err != nil {
		return 0, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
	}
	if len(sourceIDConfig) != 0 && sourceIDConfig[0].Value != "" {
		sourceID, err = strconv.ParseUint(sourceIDConfig[0].Value, 10, 64)
		if err != nil {
			log.Error("fail to parse sourceID from PD",
				zap.String("sourceID", sourceIDConfig[0].Value),
				zap.Error(err))
			return 0, cerror.WrapError(cerror.ErrPDEtcdAPIError, err)
		}
	}
	return sourceID, nil
}
