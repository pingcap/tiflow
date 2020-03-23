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

package kv

import (
	"fmt"
	"sort"

	"github.com/pingcap/ticdc/pkg/flags"
	"github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/codec"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
)

func loadHistoryDDLJobs(tiStore tidbkv.Storage) ([]*model.Job, error) {
	snapMeta, err := getSnapshotMeta(tiStore)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := snapMeta.GetAllHistoryDDLJobs()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// jobs from GetAllHistoryDDLJobs are sorted by job id, need sorted by schema version
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].BinlogInfo.FinishedTS < jobs[j].BinlogInfo.FinishedTS
	})

	return jobs, nil
}

func getSnapshotMeta(tiStore tidbkv.Storage) (*meta.Meta, error) {
	version, err := tiStore.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	snapshot, err := tiStore.GetSnapshot(version)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta.NewSnapshotMeta(snapshot), nil
}

// LoadHistoryDDLJobs loads all history DDL jobs from TiDB.
func LoadHistoryDDLJobs(kvStore tidbkv.Storage) ([]*model.Job, error) {
	originalJobs, err := loadHistoryDDLJobs(kvStore)
	jobs := make([]*model.Job, 0, len(originalJobs))
	if err != nil {
		return nil, err
	}
	tikvStorage, ok := kvStore.(tikv.Storage)
	for _, job := range originalJobs {
		if job.State != model.JobStateSynced && job.State != model.JobStateDone {
			continue
		}
		if ok {
			err := resetFinishedTs(tikvStorage, job)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func resetFinishedTs(kvStore tikv.Storage, job *model.Job) error {
	helper := helper.NewHelper(kvStore)
	diffKey := schemaDiffKey(job.BinlogInfo.SchemaVersion)
	resp, err := helper.GetMvccByEncodedKey(diffKey)
	if err != nil {
		return errors.Trace(err)
	}
	mvcc := resp.GetInfo()
	if mvcc == nil || len(mvcc.Writes) == 0 {
		return errors.NotFoundf("mvcc info, ddl job id: %d, schema version: %d", job.ID, job.BinlogInfo.SchemaVersion)
	}
	var finishedTS uint64
	for _, w := range mvcc.Writes {
		if finishedTS < w.CommitTs {
			finishedTS = w.CommitTs
		}
	}
	job.BinlogInfo.FinishedTS = finishedTS
	return nil
}

// CreateTiStore creates a new tikv storage client
func CreateTiStore(urls string) (tidbkv.Storage, error) {
	urlv, err := flags.NewURLsValue(urls)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Ignore error if it is already registered.
	_ = store.Register("tikv", tikv.Driver{})

	tiPath := fmt.Sprintf("tikv://%s?disableGC=true", urlv.HostString())
	tiStore, err := store.New(tiPath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return tiStore, nil
}

func schemaDiffKey(schemaVersion int64) []byte {
	metaPrefix := []byte("m")
	mSchemaDiffPrefix := "Diff"
	StringData := 's'
	key := []byte(fmt.Sprintf("%s:%d", mSchemaDiffPrefix, schemaVersion))

	ek := make([]byte, 0, len(metaPrefix)+len(key)+24)
	ek = append(ek, metaPrefix...)
	ek = codec.EncodeBytes(ek, key)
	return codec.EncodeUint(ek, uint64(StringData))
}
