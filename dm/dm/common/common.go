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

package common

import (
	"encoding/hex"
	"fmt"
	"path"
	"strings"

	"github.com/pingcap/dm/pkg/terror"
)

var (
	useOfClosedErrMsg = "use of closed network connection"
	// ClusterVersionKey is used to store the version of the cluster.
	ClusterVersionKey = "/dm-cluster/version"
	// WorkerRegisterKeyAdapter is used to encode and decode register key.
	// k/v: Encode(worker-name) -> the information of the DM-worker node.
	WorkerRegisterKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-worker/r/")
	// WorkerKeepAliveKeyAdapter is used to encode and decode keepalive key.
	// k/v: Encode(worker-name) -> time.
	WorkerKeepAliveKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-worker/a/")
	// LoadTaskKeyAdapter is used to store the worker which in load stage for the source of the subtask.
	// k/v: Encode(task, source-id) -> worker-name.
	LoadTaskKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/load-task/")
	// UpstreamConfigKeyAdapter stores all config of which MySQL-task has not stopped.
	// k/v: Encode(source-id) -> config.
	UpstreamConfigKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/v2/upstream/config/")
	// UpstreamBoundWorkerKeyAdapter is used to store address of worker in which MySQL-tasks which are running.
	// k/v: Encode(worker-name) -> the bound relationship.
	UpstreamBoundWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/bound-worker/")
	// UpstreamLastBoundWorkerKeyAdapter is used to store address of worker in which MySQL-tasks which are running.
	// different with UpstreamBoundWorkerKeyAdapter, this kv should not be deleted when unbound, to provide a priority
	// k/v: Encode(worker-name) -> the bound relationship.
	UpstreamLastBoundWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/last-bound-worker/")
	// UpstreamRelayWorkerKeyAdapter is used to store the upstream which this worker needs to pull relay log
	// k/v: Encode(worker-name) -> source-id.
	UpstreamRelayWorkerKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/relay-worker/")
	// UpstreamSubTaskKeyAdapter is used to store SubTask which are subscribing data from MySQL source.
	// k/v: Encode(source-id, task-name) -> SubTaskConfig.
	UpstreamSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/upstream/subtask/")
	// StageRelayKeyAdapter is used to store the running stage of the relay.
	// k/v: Encode(source-id) -> the running stage of the relay.
	StageRelayKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/v2/stage/relay/")
	// StageSubTaskKeyAdapter is used to store the running stage of the subtask.
	// k/v: Encode(source-id, task-name) -> the running stage of the subtask.
	StageSubTaskKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/stage/subtask/")

	// ShardDDLPessimismInfoKeyAdapter is used to store shard DDL info in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL info.
	ShardDDLPessimismInfoKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/info/")
	// ShardDDLPessimismOperationKeyAdapter is used to store shard DDL operation in pessimistic model.
	// k/v: Encode(task-name, source-id) -> shard DDL operation.
	ShardDDLPessimismOperationKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-pessimism/operation/")

	// ShardDDLOptimismSourceTablesKeyAdapter is used to store INITIAL upstream schema & table names when starting the subtask.
	// In other words, if any Info for this subtask exists, we should obey source tables in the Info.
	// This is because the current upstream tables may not match the tables that the binlog stream has reached.
	// k/v: Encode(task-name, source-id) -> upstream schema & table names.
	ShardDDLOptimismSourceTablesKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/source-tables/")
	// ShardDDLOptimismInfoKeyAdapter is used to store shard DDL info in optimistic model.
	// k/v: Encode(task-name, source-id, upstream-schema-name, upstream-table-name) -> shard DDL info.
	ShardDDLOptimismInfoKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/info/")
	// ShardDDLOptimismOperationKeyAdapter is used to store shard DDL operation in optimistic model.
	// k/v: Encode(task-name, source-id, upstream-schema-name, upstream-table-name) -> shard DDL operation.
	ShardDDLOptimismOperationKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/operation/")
	// ShardDDLOptimismInitSchemaKeyAdapter is used to store the initial schema (before constructed the lock) of merged tables.
	// k/v: Encode(task-name, downstream-schema-name, downstream-table-name) -> table schema.
	ShardDDLOptimismInitSchemaKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/init-schema/")
	// ShardDDLOptimismDroppedColumnsKeyAdapter is used to store the columns that are not fully dropped
	// k/v: Encode(lock-id, column-name, source-id, upstream-schema-name, upstream-table-name) -> int
	// If we don't identify different upstream tables, we may report an error for tb2 in the following case.
	// Time series: (+a/-a means add/drop column a)
	//	    older ----------------> newer
	// tb1: +a +b +c           -c
	// tb2:                       +a +b +c
	// tb3:          +a +b +c
	ShardDDLOptimismDroppedColumnsKeyAdapter KeyAdapter = keyHexEncoderDecoder("/dm-master/shardddl-optimism/dropped-columns/")
)

func keyAdapterKeysLen(s KeyAdapter) int {
	switch s {
	case WorkerRegisterKeyAdapter, UpstreamConfigKeyAdapter, UpstreamBoundWorkerKeyAdapter,
		WorkerKeepAliveKeyAdapter, StageRelayKeyAdapter,
		UpstreamLastBoundWorkerKeyAdapter, UpstreamRelayWorkerKeyAdapter:
		return 1
	case UpstreamSubTaskKeyAdapter, StageSubTaskKeyAdapter,
		ShardDDLPessimismInfoKeyAdapter, ShardDDLPessimismOperationKeyAdapter,
		ShardDDLOptimismSourceTablesKeyAdapter, LoadTaskKeyAdapter:
		return 2
	case ShardDDLOptimismInitSchemaKeyAdapter:
		return 3
	case ShardDDLOptimismInfoKeyAdapter, ShardDDLOptimismOperationKeyAdapter:
		return 4
	case ShardDDLOptimismDroppedColumnsKeyAdapter:
		return 5
	// used in upgrading
	case UpstreamConfigKeyAdapterV1, StageRelayKeyAdapterV1:
		return 1
	}
	return -1
}

// IsErrNetClosing checks whether is an ErrNetClosing error.
func IsErrNetClosing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), useOfClosedErrMsg)
}

// KeyAdapter is used to construct etcd key.
type KeyAdapter interface {
	// Encode returns a string encoded by given keys.
	// If give all keys whose number is the same as specified in keyAdapterKeysLen, it returns a non `/` terminated
	// string, and it should not be used with WithPrefix.
	// If give not enough keys, it returns a `/` terminated string that could be used with WithPrefix.
	Encode(keys ...string) string
	Decode(key string) ([]string, error)
	Path() string
}

type keyEncoderDecoder string

// always use keyHexEncoderDecoder to avoid `/` in keys.
type keyHexEncoderDecoder string

func (s keyEncoderDecoder) Encode(keys ...string) string {
	t := []string{string(s)}
	t = append(t, keys...)
	return path.Join(t...)
}

func (s keyEncoderDecoder) Decode(key string) ([]string, error) {
	v := strings.TrimPrefix(key, string(s))
	vals := strings.Split(v, "/")
	if l := keyAdapterKeysLen(s); l != len(vals) {
		return nil, terror.ErrDecodeEtcdKeyFail.Generate(fmt.Sprintf("decoder is %s, the key is %s", string(s), key))
	}
	return vals, nil
}

func (s keyEncoderDecoder) Path() string {
	return string(s)
}

func (s keyHexEncoderDecoder) Encode(keys ...string) string {
	hexKeys := []string{string(s)}
	for _, key := range keys {
		hexKeys = append(hexKeys, hex.EncodeToString([]byte(key)))
	}
	ret := path.Join(hexKeys...)
	if len(keys) < keyAdapterKeysLen(s) {
		ret += "/"
	}
	return ret
}

func (s keyHexEncoderDecoder) Decode(key string) ([]string, error) {
	if key[len(key)-1] == '/' {
		key = key[:len(key)-1]
	}
	v := strings.Split(strings.TrimPrefix(key, string(s)), "/")
	if l := keyAdapterKeysLen(s); l != len(v) {
		return nil, terror.ErrDecodeEtcdKeyFail.Generate(fmt.Sprintf("decoder is %s, the key is %s", string(s), key))
	}
	for i, k := range v {
		dec, err := hex.DecodeString(k)
		if err != nil {
			return nil, terror.ErrDecodeEtcdKeyFail.Generate(err.Error())
		}
		v[i] = string(dec)
	}
	return v, nil
}

func (s keyHexEncoderDecoder) Path() string {
	return string(s)
}

// used in upgrade.
var (
	// UpstreamConfigKeyAdapter stores all config of which MySQL-task has not stopped.
	// k/v: Encode(source-id) -> config.
	UpstreamConfigKeyAdapterV1 KeyAdapter = keyEncoderDecoder("/dm-master/upstream/config/")
	// StageRelayKeyAdapter is used to store the running stage of the relay.
	// k/v: Encode(source-id) -> the running stage of the relay.
	StageRelayKeyAdapterV1 KeyAdapter = keyEncoderDecoder("/dm-master/stage/relay/")
)

// NoSubTaskMsg returns a formatted string for subtask not started.
func NoSubTaskMsg(name string) string {
	return fmt.Sprintf("no sub task with name %s has started", name)
}
