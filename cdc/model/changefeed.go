// Copyright 2019 PingCAP, Inc.
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

package model

import (
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

// ChangeFeedInfo describes the detail of a ChangeFeed
type ChangeFeedInfo struct {
	SinkURI    string            `json:"sink-uri"`
	Opts       map[string]string `json:"opts"`
	CreateTime time.Time         `json:"create-time"`
	// Start sync at this commit ts if `StartTs` is specify or using the CreateTime of changefeed.
	StartTs uint64 `json:"start-ts"`
	// The ChangeFeed will exits until sync to timestamp TargetTs
	TargetTs uint64 `json:"target-ts"`
	// used for admin job notification, trigger watch event in capture
	AdminJobType AdminJobType      `json:"admin-job-type"`
	Status       *ChangeFeedStatus `json:"-"`

	filter *filter.Filter
	Config *ReplicaConfig `json:"config"`
}

func (info *ChangeFeedInfo) getConfig() *ReplicaConfig {
	if info.Config == nil {
		info.Config = &ReplicaConfig{}
	}
	return info.Config
}

func (info *ChangeFeedInfo) getFilter() *filter.Filter {
	if info.filter == nil {
		rules := info.getConfig().FilterRules
		info.filter = filter.New(info.getConfig().FilterCaseSensitive, rules)
	}
	return info.filter
}

// ShouldIgnoreTxn returns true is the given txn should be ignored
func (info *ChangeFeedInfo) ShouldIgnoreTxn(t *Txn) bool {
	for _, ignoreTs := range info.getConfig().IgnoreTxnCommitTs {
		if ignoreTs == t.Ts {
			return true
		}
	}
	return false
}

// ShouldIgnoreTable returns true if the specified table should be ignored by this change feed.
// Set `tbl` to an empty string to test against the whole database.
func (info *ChangeFeedInfo) ShouldIgnoreTable(db, tbl string) bool {
	if IsSysSchema(db) {
		return true
	}
	f := info.getFilter()
	// TODO: Change filter to support simple check directly
	left := f.ApplyOn([]*filter.Table{{Schema: db, Name: tbl}})
	return len(left) == 0
}

// FilterTxn removes DDL/DMLs that's not wanted by this change feed.
// CDC only supports filtering by database/table now.
func (info *ChangeFeedInfo) FilterTxn(t *Txn) {
	if t.IsDDL() {
		if info.ShouldIgnoreTable(t.DDL.Database, t.DDL.Table) {
			t.DDL = nil
		}
	} else {
		var filteredDMLs []*DML
		for _, dml := range t.DMLs {
			if !info.ShouldIgnoreTable(dml.Database, dml.Table) {
				filteredDMLs = append(filteredDMLs, dml)
			}
		}
		t.DMLs = filteredDMLs
	}
}

// GetStartTs returns StartTs if it's  specified or using the CreateTime of changefeed.
func (info *ChangeFeedInfo) GetStartTs() uint64 {
	if info.StartTs > 0 {
		return info.StartTs
	}

	return oracle.EncodeTSO(info.CreateTime.Unix() * 1000)
}

// GetTargetTs returns TargetTs if it's specified, otherwise MaxUint64 is returned.
func (info *ChangeFeedInfo) GetTargetTs() uint64 {
	if info.TargetTs > 0 {
		return info.TargetTs
	}
	return uint64(math.MaxUint64)
}

// GetCheckpointTs returns the checkpoint ts of changefeed.
func (info *ChangeFeedInfo) GetCheckpointTs() uint64 {
	if info.Status != nil {
		return info.Status.CheckpointTs
	}

	return info.GetStartTs()
}

// Marshal returns the json marshal format of a ChangeFeedInfo
func (info *ChangeFeedInfo) Marshal() (string, error) {
	data, err := json.Marshal(info)
	return string(data), errors.Trace(err)
}

// Unmarshal unmarshals into *ChangeFeedInfo from json marshal byte slice
func (info *ChangeFeedInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &info)
	return errors.Annotatef(err, "Unmarshal data: %v", data)
}

// IsSysSchema returns true if the given schema is a system schema
func IsSysSchema(db string) bool {
	db = strings.ToUpper(db)
	for _, schema := range []string{"INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "MYSQL", "METRIC_SCHEMA"} {
		if schema == db {
			return true
		}
	}
	return false
}
