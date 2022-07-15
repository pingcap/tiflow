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

package entry

import (
	"github.com/pingcap/errors"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
)

// VerifyTables catalog tables specified by ReplicaConfig into
// eligible (has an unique index or primary key) and ineligible tables.
func VerifyTables(replicaConfig *config.ReplicaConfig,
	storage tidbkv.Storage, startTs uint64) (ineligibleTables,
	eligibleTables []model.TableName, err error,
) {
	filter, err := filter.NewFilter(replicaConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	meta, err := kv.GetSnapshotMeta(storage, startTs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	snap, err := schema.NewSingleSnapshotFromMeta(meta, startTs, false /* explicitTables */)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	snap.IterTables(true, func(tableInfo *model.TableInfo) {
		if filter.ShouldIgnoreTable(tableInfo.TableName.Schema, tableInfo.TableName.Table) {
			return
		}
		// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
		// See https://github.com/pingcap/tiflow/issues/4559
		if tableInfo.IsSequence() {
			return
		}
		if !tableInfo.IsEligible(false /* forceReplicate */) {
			ineligibleTables = append(ineligibleTables, tableInfo.TableName)
		} else {
			eligibleTables = append(eligibleTables, tableInfo.TableName)
		}
	})
	return
}
