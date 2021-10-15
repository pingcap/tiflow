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
	"encoding/json"

	"github.com/pingcap/parser/model"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/clientv3util"

	"github.com/pingcap/dm/dm/common"
	"github.com/pingcap/dm/pkg/etcdutil"
)

// InitSchema represents the initial schema (schema before the lock constructed) of a merged table.
// NOTE: `Task`, `DownSchema` and `DownTable` are redundant in the etcd key path for convenient.
type InitSchema struct {
	Task       string           `json:"task"`        // data migration task name
	DownSchema string           `json:"down-schema"` // downstream/target schema name
	DownTable  string           `json:"down-table"`  // downstream/target table name
	TableInfo  *model.TableInfo `json:"table-info"`  // the initial table info (schema)
}

// NewInitSchema creates a new InitSchema instance.
func NewInitSchema(task, downSchema, downTable string, tableInfo *model.TableInfo) InitSchema {
	return InitSchema{
		Task:       task,
		DownSchema: downSchema,
		DownTable:  downTable,
		TableInfo:  tableInfo,
	}
}

// String implements Stringer interface.
func (is InitSchema) String() string {
	s, _ := is.toJSON()
	return s
}

// toJSON returns the string of JSON represent.
func (is InitSchema) toJSON() (string, error) {
	data, err := json.Marshal(is)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// IsEmpty returns true when this InitSchema has no value.
func (is InitSchema) IsEmpty() bool {
	var emptyIS InitSchema
	return is == emptyIS
}

// infoFromJSON constructs InitSchema from its JSON represent.
func initSchemaFromJSON(s string) (is InitSchema, err error) {
	err = json.Unmarshal([]byte(s), &is)
	return
}

// GetInitSchema gets the InitSchema for the specified downstream table.
func GetInitSchema(cli *clientv3.Client, task, downSchema, downTable string) (InitSchema, int64, error) {
	var is InitSchema
	op := clientv3.OpGet(common.ShardDDLOptimismInitSchemaKeyAdapter.Encode(task, downSchema, downTable))
	respTxn, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	if err != nil {
		return is, 0, err
	}
	resp := respTxn.Responses[0].GetResponseRange()

	if resp.Count > 0 {
		is, err = initSchemaFromJSON(string(resp.Kvs[0].Value))
		if err != nil {
			return is, 0, err
		}
	}
	return is, rev, nil
}

// GetAllInitSchemas gets all init schemas from etcd.
// This function should often be called by DM-master.
// k/k/k/v: task-name -> downstream-schema-name -> downstream-table-name -> InitSchema.
func GetAllInitSchemas(cli *clientv3.Client) (map[string]map[string]map[string]InitSchema, int64, error) {
	initSchemas := make(map[string]map[string]map[string]InitSchema)
	op := clientv3.OpGet(common.ShardDDLOptimismInitSchemaKeyAdapter.Path(), clientv3.WithPrefix())
	respTxn, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	if err != nil {
		return nil, 0, err
	}
	resp := respTxn.Responses[0].GetResponseRange()

	for _, kv := range resp.Kvs {
		schema, err := initSchemaFromJSON(string(kv.Value))
		if err != nil {
			return nil, 0, err
		}
		if _, ok := initSchemas[schema.Task]; !ok {
			initSchemas[schema.Task] = make(map[string]map[string]InitSchema)
		}
		if _, ok := initSchemas[schema.Task][schema.DownSchema]; !ok {
			initSchemas[schema.Task][schema.DownSchema] = make(map[string]InitSchema)
		}
		initSchemas[schema.Task][schema.DownSchema][schema.DownTable] = schema
	}
	return initSchemas, rev, nil
}

// PutInitSchemaIfNotExist puts the InitSchema into ectd if no previous one exists.
func PutInitSchemaIfNotExist(cli *clientv3.Client, is InitSchema) (rev int64, putted bool, err error) {
	value, err := is.toJSON()
	if err != nil {
		return 0, false, err
	}
	key := common.ShardDDLOptimismInitSchemaKeyAdapter.Encode(is.Task, is.DownSchema, is.DownTable)

	cmp := clientv3util.KeyMissing(key)
	op := clientv3.OpPut(key, value)

	resp, rev, err := etcdutil.DoOpsInOneCmpsTxnWithRetry(cli, []clientv3.Cmp{cmp}, []clientv3.Op{op}, []clientv3.Op{})
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// DeleteInitSchema tries to delete the InitSchema for the specified downstream table.
func DeleteInitSchema(cli *clientv3.Client, task, downSchema, downTable string) (rev int64, deleted bool, err error) {
	op := deleteInitSchemaOp(task, downSchema, downTable)
	resp, rev, err := etcdutil.DoOpsInOneTxnWithRetry(cli, op)
	if err != nil {
		return 0, false, err
	}
	return rev, resp.Succeeded, nil
}

// deleteInitSchemaOp returns a DELETE etcd operation for init schema.
func deleteInitSchemaOp(task, downSchema, downTable string) clientv3.Op {
	return clientv3.OpDelete(common.ShardDDLOptimismInitSchemaKeyAdapter.Encode(task, downSchema, downTable))
}
