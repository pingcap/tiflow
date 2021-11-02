// Copyright 2021 PingCAP, Inc.
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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplicaConfigMarshal(t *testing.T) {
	t.Parallel()
	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	b, err := conf.Marshal()
	require.Nil(t, err)
	require.Equal(t, `{"case-sensitive":false,"enable-old-value":true,"force-replicate":true,"check-gc-safe-point":true,"filter":{"rules":["1.1"],"ignore-txn-start-ts":null},"mounter":{"worker-num":3},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1},"consistent":{"level":"none","max-log-size":64,"flush-interval":1000,"storage":""}}`, b)
	conf2 := new(ReplicaConfig)
	err = conf2.Unmarshal([]byte(`{"case-sensitive":false,"enable-old-value":true,"force-replicate":true,"check-gc-safe-point":true,"filter":{"rules":["1.1"],"ignore-txn-start-ts":null},"mounter":{"worker-num":3},"sink":{"dispatchers":null,"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1},"consistent":{"level":"none","max-log-size":64,"flush-interval":1000,"storage":""}}`))
	require.Nil(t, err)
	require.Equal(t, conf, conf2)
}

func TestReplicaConfigClone(t *testing.T) {
	t.Parallel()
	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	conf2 := conf.Clone()
	require.Equal(t, conf, conf2)
	conf2.Mounter.WorkerNum = 4
	require.Equal(t, 3, conf.Mounter.WorkerNum)
}

func TestReplicaConfigOutDated(t *testing.T) {
	t.Parallel()
	conf2 := new(ReplicaConfig)
	err := conf2.Unmarshal([]byte(`{"case-sensitive":false,"enable-old-value":true,"force-replicate":true,"check-gc-safe-point":true,"filter":{"rules":["1.1"],"ignore-txn-start-ts":null,"ddl-allow-list":null},"mounter":{"worker-num":3},"sink":{"dispatch-rules":[{"db-name":"a","tbl-name":"b","rule":"r1"},{"db-name":"a","tbl-name":"c","rule":"r2"},{"db-name":"a","tbl-name":"d","rule":"r2"}],"protocol":"default"},"cyclic-replication":{"enable":false,"replica-id":0,"filter-replica-ids":null,"id-buckets":0,"sync-ddl":false},"scheduler":{"type":"table-number","polling-time":-1},"consistent":{"level":"none","max-log-size":64,"flush-interval":1000,"storage":""}}`))
	require.Nil(t, err)

	conf := GetDefaultReplicaConfig()
	conf.CaseSensitive = false
	conf.ForceReplicate = true
	conf.Filter.Rules = []string{"1.1"}
	conf.Mounter.WorkerNum = 3
	conf.Sink.DispatchRules = []*DispatchRule{
		{Matcher: []string{"a.b"}, Dispatcher: "r1"},
		{Matcher: []string{"a.c"}, Dispatcher: "r2"},
		{Matcher: []string{"a.d"}, Dispatcher: "r2"},
	}
	require.Equal(t, conf, conf2)
}

func TestServerConfigMarshal(t *testing.T) {
	t.Parallel()

	rawConfig := `{"addr":"192.155.22.33:8887","advertise-addr":"","log-file":"","log-level":"info","log-http":true,"log":{"file":{"max-size":300,"max-days":0,"max-backups":0}},"data-dir":"","gc-ttl":86400,"tz":"System","capture-session-ttl":10,"owner-flush-interval":200000000,"processor-flush-interval":100000000,"sorter":{"num-concurrent-worker":4,"chunk-size-limit":999,"max-memory-percentage":30,"max-memory-consumption":17179869184,"num-workerpool-goroutine":16,"sort-dir":"/tmp/sorter"},"security":{"ca-path":"","cert-path":"","key-path":"","cert-allowed-cn":null},"per-table-memory-quota":10485760,"kv-client":{"worker-concurrent":8,"worker-pool-size":0,"region-scan-limit":40}}`

	conf := GetDefaultServerConfig()
	conf.Addr = "192.155.22.33:8887"
	conf.Sorter.ChunkSizeLimit = 999
	b, err := conf.Marshal()
	require.Nil(t, err)

	require.Equal(t, rawConfig, b)
	conf2 := new(ServerConfig)
	err = conf2.Unmarshal([]byte(rawConfig))
	require.Nil(t, err)
	require.Equal(t, conf, conf2)
}

func TestServerConfigClone(t *testing.T) {
	t.Parallel()
	conf := GetDefaultServerConfig()
	conf.Addr = "192.155.22.33:8887"
	conf.Sorter.ChunkSizeLimit = 999
	conf2 := conf.Clone()
	require.Equal(t, conf, conf2)
	conf.Sorter.ChunkSizeLimit = 99
	require.Equal(t, uint64(99), conf.Sorter.ChunkSizeLimit)
}

func TestServerConfigValidateAndAdjust(t *testing.T) {
	t.Parallel()
	conf := new(ServerConfig)

	require.Regexp(t, ".*empty address", conf.ValidateAndAdjust())
	conf.Addr = "cdc:1234"
	require.Regexp(t, ".*empty GC TTL is not allowed", conf.ValidateAndAdjust())
	conf.GcTTL = 60
	require.Nil(t, conf.ValidateAndAdjust())
	require.Equal(t, conf.Addr, conf.AdvertiseAddr)
	conf.AdvertiseAddr = "advertise:1234"
	require.Nil(t, conf.ValidateAndAdjust())
	require.Equal(t, "cdc:1234", conf.Addr)
	require.Equal(t, "advertise:1234", conf.AdvertiseAddr)
	conf.AdvertiseAddr = "0.0.0.0:1234"
	require.Regexp(t, ".*must be specified.*", conf.ValidateAndAdjust())
	conf.Addr = "0.0.0.0:1234"
	require.Regexp(t, ".*must be specified.*", conf.ValidateAndAdjust())
	conf.AdvertiseAddr = "advertise"
	require.Regexp(t, ".*does not contain a port", conf.ValidateAndAdjust())
	conf.AdvertiseAddr = "advertise:1234"
	conf.PerTableMemoryQuota = 1
	require.Nil(t, conf.ValidateAndAdjust())
	require.EqualValues(t, 1, conf.PerTableMemoryQuota)
	conf.PerTableMemoryQuota = 0
	require.Nil(t, conf.ValidateAndAdjust())
	require.EqualValues(t, GetDefaultServerConfig().PerTableMemoryQuota, conf.PerTableMemoryQuota)
}
