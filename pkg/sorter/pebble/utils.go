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

package pebble

import (
	"strconv"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sorter/pebble/encoding"
)

const (
	minTableCRTsLabel      string = "minCRTs"
	maxTableCRTsLabel      string = "maxCRTs"
	tableCRTsCollectorName string = "table-crts-collector"
)

func iterTable(
	db *pebble.DB,
	uniqueID uint32, tableID model.TableID,
	lowerBound, upperBound model.Ts,
) *pebble.Iterator {
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: encoding.EncodeTsKey(uniqueID, uint64(tableID), lowerBound),
		UpperBound: encoding.EncodeTsKey(uniqueID, uint64(tableID), upperBound),
		TableFilter: func(userProps map[string]string) bool {
			tableMinCRTs, _ := strconv.Atoi(userProps[minTableCRTsLabel])
			tableMaxCRTs, _ := strconv.Atoi(userProps[maxTableCRTsLabel])
			return uint64(tableMaxCRTs) >= lowerBound && uint64(tableMinCRTs) < upperBound
		},
	})
	iter.First()
	return iter
}
