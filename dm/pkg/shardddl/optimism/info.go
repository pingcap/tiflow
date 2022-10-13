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

package optimism

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/schemacmp"
	"github.com/pingcap/tiflow/dm/common"
	"github.com/pingcap/tiflow/dm/pkg/etcdutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TODO: much of the code in optimistic mode is very similar to pessimistic mode, we can try to combine them together.

// Info represents the shard DDL information.
// This information should be persistent in etcd so can be retrieved after the DM-master leader restarted or changed.
// NOTE: `Task`, `Source`, `UpSchema` and `DownTable` are redundant in the etcd key path for convenient.
// Info is putted when receiving a shard DDL for a table in DM-worker,
// and is deleted when removing the lock by DM-master
// because we need the newest schema in Info to recover the lock when restarting DM-master.
// when new Info is putted to overwrite the old one, the DM-master should update the lock based on the new one.
type Info struct {
	Task       string   `json:"task"`        // data migration task name
	Source     string   `json:"source"`      // upstream source ID
	UpSchema   string   `json:"up-schema"`   // upstream/source schema name, different sources can have the same schema name
	UpTable    string   `json:"up-table"`    // upstream/source table name, different sources can have the same table name
	DownSchema string   `json:"down-schema"` // downstream/target schema name
	DownTable  string   `json:"down-table"`  // downstream/target table name
	DDLs       []string `json:"ddls"`        // DDL statements

	TableInfoBefore *model.TableInfo   `json:"table-info-before"` // the tracked table schema before applying the DDLs
	TableInfosAfter []*model.TableInfo `json:"table-info-after"`  // the tracked table schema after applying the DDLs

	// only used to report to the caller of the watcher, do not marsh it.
	// if it's true, it means the Info has been deleted in etcd.
	IsDeleted bool `json:"-"`

	// only set it when get/watch from etcd
	Version int64 `json:"-"`

	// only set it when get from etcd
	// use for sort infos in recoverlock
	Revision int64 `json:"-"`

	// use to resolve conflict
	IgnoreConflict bool `json:"ignore-conflict"`
}

// LogInfo replace TableInfo with schema.Table.String() for log.
type LogInfo struct {
	Task           string   `json:"task"`
	Source         string   `json:"source"`
	UpSchema       string   `json:"up-schema"`
	UpTable        string   `json:"up-table"`
	DownSchema     string   `json:"down-schema"`
	DownTable      string   `json:"down-table"`
	DDLs           []string `json:"ddls"`
	TableBefore    string   `json:"table-before"`
	TableAfter     string   `json:"table-after"`
	IsDeleted      bool     `json:"is-deleted"`
	Version        int64    `json:"version"`
	Revision       int64    `json:"revision"`
	IgnoreConflict bool     `json:"ignore-conflict"`
}

// NewInfo creates a new Info instance.
func NewInfo(task, source, upSchema, upTable, downSchema, downTable string,
	ddls []string, tableInfoBefore *model.TableInfo, tableInfosAfter []*model.TableInfo,
) Info {
	return Info{
		Task:            task,
		Source:          source,
		UpSchema:        upSchema,
		UpTable:         upTable,
		DownSchema:      downSchema,
		DownTable:       downTable,
		DDLs:            ddls,
		TableInfoBefore: tableInfoBefore,
		TableInfosAfter: tableInfosAfter,
	}
}

// String implements Stringer interface.
func (i Info) String() string {
	s, _ := i.toJSON()
	return s
}

// ShortString returns short string of Info.
func (i *Info) ShortString() string {
	logInfo := LogInfo{
		Task:           i.Task,
		Source:         i.Source,
		UpSchema:       i.UpSchema,
		UpTable:        i.UpTable,
		DownSchema:     i.DownSchema,
		DownTable:      i.DownTable,
		DDLs:           i.DDLs,
		IsDeleted:      i.IsDeleted,
		Version:        i.Version,
		Revision:       i.Revision,
		IgnoreConflict: i.IgnoreConflict,
	}
	if i.TableInfoBefore != nil {
		logInfo.TableBefore = schemacmp.Encode(i.TableInfoBefore).String()
	}
	if len(i.TableInfosAfter) != 0 {
		logInfo.TableAfter = schemacmp.Encode(i.TableInfosAfter[len(i.TableInfosAfter)-1]).String()
	}
	s, _ := logInfo.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (i LogInfo) toJSON() (string, error) {
	data, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// toJSON returns the string of JSON represent.
func (i Info) toJSON() (string, error) {
	data, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// infoFromJSON constructs Info from its JSON represent.
func infoFromJSON(s string) (i Info, err error) {
	err = json.Unmarshal([]byte(s), &i)
	if err != nil {
		// For compatibility.
		// In v2.0.2, we changed struct of table-info-after but forgot to upgrade etcd value.
		// To keep the ModRevision of info, we change them after getting info instead of change all the value in etcd when upgrade
		// All the Info will be upgraded after new info putted or lock resolved.
		oldInfo, newErr := oldInfoFromJSON(s)
		if newErr != nil {
			log.L().Error("unmarshal old info", log.ShortError(newErr))
			return
		}
		return oldInfo.toInfo(), nil
	}
	return
}

// PutInfo puts the shard DDL info into etcd.
// NOTE:
//
//	In some cases before the lock resolved, the same DDL info may be PUT multiple times:
//	  1. start-task after stop-task.
//	  2. resume-task after paused manually or automatically.
//	  3. the task scheduled to another DM-worker instance (just like case-1).
//	Then we need to ensure re-PUT is safe:
//	  1. DM-master can construct the lock and do the coordination correctly.
//	  2. DM-worker can re-PUT and comply with the coordination correctly.
//
// This function should often be called by DM-worker.
func PutInfo(cli *clientv3.Client, info Info) (int64, error) {
	op, err := putInfoOp(info)
	if err != nil {
		return 0, err
	}
	_, rev, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(op))
	return rev, err
}

// GetAllInfo gets all shard DDL info in etcd currently.
// This function should often be called by DM-master.
// k/k/k/k/v: task-name -> source-ID -> upstream-schema-name -> upstream-table-name -> shard DDL info.
// ugly code, but have no better idea now.
func GetAllInfo(cli *clientv3.Client) (map[string]map[string]map[string]map[string]Info, int64, error) {
	respTxn, _, err := etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(clientv3.OpGet(common.ShardDDLOptimismInfoKeyAdapter.Path(), clientv3.WithPrefix())))
	if err != nil {
		return nil, 0, err
	}
	resp := respTxn.Responses[0].GetResponseRange()

	ifm := make(map[string]map[string]map[string]map[string]Info)
	for _, kv := range resp.Kvs {
		info, err2 := infoFromJSON(string(kv.Value))
		if err2 != nil {
			return nil, 0, err2
		}
		info.Version = kv.Version
		info.Revision = kv.ModRevision

		if _, ok := ifm[info.Task]; !ok {
			ifm[info.Task] = make(map[string]map[string]map[string]Info)
		}
		if _, ok := ifm[info.Task][info.Source]; !ok {
			ifm[info.Task][info.Source] = make(map[string]map[string]Info)
		}
		if _, ok := ifm[info.Task][info.Source][info.UpSchema]; !ok {
			ifm[info.Task][info.Source][info.UpSchema] = make(map[string]Info)
		}
		ifm[info.Task][info.Source][info.UpSchema][info.UpTable] = info
	}

	return ifm, resp.Header.Revision, nil
}

// WatchInfo watches PUT & DELETE operations for info.
// This function should often be called by DM-master.
func WatchInfo(ctx context.Context, cli *clientv3.Client, revision int64,
	outCh chan<- Info, errCh chan<- error,
) {
	wCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// NOTE: WithPrevKV used to get a valid `ev.PrevKv` for deletion.
	ch := cli.Watch(wCtx, common.ShardDDLOptimismInfoKeyAdapter.Path(),
		clientv3.WithPrefix(), clientv3.WithRev(revision), clientv3.WithPrevKV())

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				return
			}
			if resp.Canceled {
				select {
				case errCh <- resp.Err():
				case <-ctx.Done():
				}
				return
			}

			for _, ev := range resp.Events {
				var (
					info Info
					err  error
				)

				switch ev.Type {
				case mvccpb.PUT:
					info, err = infoFromJSON(string(ev.Kv.Value))
					info.Version = ev.Kv.Version
					info.Revision = ev.Kv.ModRevision
				case mvccpb.DELETE:
					info, err = infoFromJSON(string(ev.PrevKv.Value))
					info.IsDeleted = true
				default:
					// this should not happen.
					err = fmt.Errorf("unsupported ectd event type %v", ev.Type)
				}

				if err != nil {
					select {
					case errCh <- err:
					case <-ctx.Done():
						return
					}
				} else {
					select {
					case outCh <- info:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// putInfoOp returns a PUT etcd operation for Info.
func putInfoOp(info Info) (clientv3.Op, error) {
	value, err := info.toJSON()
	if err != nil {
		return clientv3.Op{}, err
	}
	key := common.ShardDDLOptimismInfoKeyAdapter.Encode(info.Task, info.Source, info.UpSchema, info.UpTable)
	return clientv3.OpPut(key, value), nil
}

// deleteInfoOp returns a DELETE etcd operation for info.
// This operation should often be sent by DM-worker.
func deleteInfoOp(info Info) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismInfoKeyAdapter.Encode(
		info.Task, info.Source, info.UpSchema, info.UpTable))
}

// ClearTestInfoOperationColumns is used to clear all shard DDL information in optimism mode.
// it only used for testing now.
func ClearTestInfoOperationColumn(cli *clientv3.Client) error {
	clearSource := clientv3.OpDelete(common.ShardDDLOptimismSourceTablesKeyAdapter.Path(), clientv3.WithPrefix())
	clearInfo := clientv3.OpDelete(common.ShardDDLOptimismInfoKeyAdapter.Path(), clientv3.WithPrefix())
	clearOp := clientv3.OpDelete(common.ShardDDLOptimismOperationKeyAdapter.Path(), clientv3.WithPrefix())
	clearColumns := clientv3.OpDelete(common.ShardDDLOptimismDroppedColumnsKeyAdapter.Path(), clientv3.WithPrefix())
	_, err := cli.Txn(context.Background()).Then(clearSource, clearInfo, clearOp, clearColumns).Commit()
	return err
}

// OldInfo represents info in etcd before v2.0.2.
type OldInfo struct {
	Task       string   `json:"task"`
	Source     string   `json:"source"`
	UpSchema   string   `json:"up-schema"`
	UpTable    string   `json:"up-table"`
	DownSchema string   `json:"down-schema"`
	DownTable  string   `json:"down-table"`
	DDLs       []string `json:"ddls"`

	TableInfoBefore *model.TableInfo `json:"table-info-before"` // the tracked table schema before applying the DDLs
	TableInfoAfter  *model.TableInfo `json:"table-info-after"`  // the tracked table schema after applying the DDLs
}

// oldInfoFromJSON constructs OldInfo from its JSON represent.
func oldInfoFromJSON(s string) (oldInfo OldInfo, err error) {
	err = json.Unmarshal([]byte(s), &oldInfo)
	return
}

// toInfo converts OldInfo to Info.
func (oldInfo *OldInfo) toInfo() Info {
	return Info{
		Task:            oldInfo.Task,
		Source:          oldInfo.Source,
		UpSchema:        oldInfo.UpSchema,
		UpTable:         oldInfo.UpTable,
		DownSchema:      oldInfo.DownSchema,
		DownTable:       oldInfo.DownTable,
		DDLs:            oldInfo.DDLs,
		TableInfoBefore: oldInfo.TableInfoBefore,
		TableInfosAfter: []*model.TableInfo{oldInfo.TableInfoAfter},
	}
}

// CheckDDLInfos try to check and fix all the schema and table names for DDL info.
func CheckDDLInfos(cli *clientv3.Client, source string, schemaMap map[string]string, tablesMap map[string]map[string]string) error {
	allInfos, _, err := GetAllInfo(cli)
	if err != nil {
		return err
	}

	for _, taskTableInfos := range allInfos {
		sourceInfos, ok := taskTableInfos[source]
		if !ok {
			continue
		}
		for schema, tblInfos := range sourceInfos {
			realSchema, hasChange := schemaMap[schema]
			if !hasChange {
				realSchema = schema
			}

			tblMap := tablesMap[schema]
			for tbl, info := range tblInfos {
				realTable, tableChange := tblMap[tbl]
				if !tableChange {
					realTable = tbl
					tableChange = hasChange
				}
				if tableChange {
					delOp := deleteInfoOp(info)
					info.UpSchema = realSchema
					info.UpTable = realTable
					putOp, err := putInfoOp(info)
					if err != nil {
						return err
					}
					_, _, err = etcdutil.DoTxnWithRepeatable(cli, etcdutil.ThenOpFunc(delOp, putOp))
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
