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
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/filter"
)

// VerifyTables catalog tables specified by ReplicaConfig into
// eligible (has an unique index or primary key) and ineligible tables.
func VerifyTables(
	f filter.Filter,
	storage tidbkv.Storage,
	startTs uint64) (
	tableInfos []*model.TableInfo,
	ineligibleTables,
	eligibleTables []model.TableName,
	err error,
) {
	meta := kv.GetSnapshotMeta(storage, startTs)
	snap, err := schema.NewSingleSnapshotFromMeta(
		model.ChangeFeedID4Test("api", "verifyTable"),
		meta, startTs, false /* explicitTables */, f)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	snap.IterTables(true, func(tableInfo *model.TableInfo) {
		if f.ShouldIgnoreTable(tableInfo.TableName.Schema, tableInfo.TableName.Table) {
			return
		}
		// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
		// See https://github.com/pingcap/tiflow/issues/4559
		if tableInfo.IsSequence() {
			return
		}
		tableInfos = append(tableInfos, tableInfo)
		if !tableInfo.IsEligible(false /* forceReplicate */) {
			ineligibleTables = append(ineligibleTables, tableInfo.TableName)
		} else {
			eligibleTables = append(eligibleTables, tableInfo.TableName)
		}
	})
	return
}
