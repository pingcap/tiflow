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

const (
	testCfgTestReplicaConfigOutDated = `{
  "case-sensitive": false,
  "enable-old-value": true,
  "force-replicate": true,
  "check-gc-safe-point": true,
  "filter": {
    "rules": [
      "1.1"
    ],
    "ignore-txn-start-ts": null,
    "ddl-allow-list": null
  },
  "mounter": {
    "worker-num": 3
  },
  "sink": {
    "dispatch-rules": [
      {
        "db-name": "a",
        "tbl-name": "b",
        "rule": "r1"
      },
      {
        "db-name": "a",
        "tbl-name": "c",
        "rule": "r2"
      },
      {
        "db-name": "a",
        "tbl-name": "d",
        "rule": "r2"
      }
    ],
    "protocol": "default"
  },
  "cyclic-replication": {
    "enable": false,
    "replica-id": 0,
    "filter-replica-ids": null,
    "id-buckets": 0,
    "sync-ddl": false
  },
  "scheduler": {
    "type": "table-number",
    "polling-time": -1
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 1000,
    "storage": ""
  }
}`

	testCfgTestServerConfigMarshal = `{
  "addr": "192.155.22.33:8887",
  "advertise-addr": "",
  "log-file": "",
  "log-level": "info",
  "log": {
    "file": {
      "max-size": 300,
      "max-days": 0,
      "max-backups": 0
    }
  },
  "data-dir": "",
  "gc-ttl": 86400,
  "tz": "System",
  "capture-session-ttl": 10,
  "owner-flush-interval": 200000000,
  "processor-flush-interval": 100000000,
  "sorter": {
    "num-concurrent-worker": 4,
    "chunk-size-limit": 999,
    "max-memory-percentage": 30,
    "max-memory-consumption": 17179869184,
    "num-workerpool-goroutine": 16,
    "sort-dir": "/tmp/sorter"
  },
  "security": {
    "ca-path": "",
    "cert-path": "",
    "key-path": "",
    "cert-allowed-cn": null
  },
  "per-table-memory-quota": 10485760,
  "kv-client": {
    "worker-concurrent": 8,
    "worker-pool-size": 0,
    "region-scan-limit": 40
  },
  "debug": {
    "enable-table-actor": true,
    "enable-db-sorter": false,
    "db": {
      "count": 16,
      "concurrency": 256,
      "max-open-files": 10000,
      "block-size": 65536,
      "block-cache-size": 4294967296,
      "writer-buffer-size": 8388608,
      "compression": "snappy",
      "target-file-size-base": 8388608,
      "compaction-l0-trigger": 160,
      "write-l0-slowdown-trigger": 2147483647,
      "write-l0-pause-trigger": 2147483647,
      "cleanup-speed-limit": 10000
    },
    "messages": {
      "client-max-batch-interval": 100000000,
      "client-max-batch-size": 8192,
      "client-max-batch-count": 128,
      "client-retry-rate-limit": 1,
      "server-max-pending-message-count": 102400,
      "server-ack-interval": 100000000,
      "server-worker-pool-size": 4
    }
  }
}`

	testCfgTestReplicaConfigMarshal1 = `{
  "case-sensitive": false,
  "enable-old-value": true,
  "force-replicate": true,
  "check-gc-safe-point": true,
  "filter": {
    "rules": [
      "1.1"
    ],
    "ignore-txn-start-ts": null
  },
  "mounter": {
    "worker-num": 3
  },
  "sink": {
    "dispatchers": null,
    "protocol": "default",
    "column-selectors": [
      {
        "matcher": [
          "1.1"
        ],
        "columns": [
          "a",
          "b"
        ]
      }
    ]
  },
  "cyclic-replication": {
    "enable": false,
    "replica-id": 0,
    "filter-replica-ids": null,
    "id-buckets": 0,
    "sync-ddl": false
  },
  "scheduler": {
    "type": "table-number",
    "polling-time": -1
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 1000,
    "storage": ""
  }
}`

	testCfgTestReplicaConfigMarshal2 = `{
  "case-sensitive": false,
  "enable-old-value": true,
  "force-replicate": true,
  "check-gc-safe-point": true,
  "filter": {
    "rules": [
      "1.1"
    ],
    "ignore-txn-start-ts": null
  },
  "mounter": {
    "worker-num": 3
  },
  "sink": {
    "dispatchers": null,
    "protocol": "default",
    "column-selectors": [
      {
        "matcher": [
          "1.1"
        ],
        "columns": [
          "a",
          "b"
        ]
      }
    ]
  },
  "cyclic-replication": {
    "enable": false,
    "replica-id": 0,
    "filter-replica-ids": null,
    "id-buckets": 0,
    "sync-ddl": false
  },
  "scheduler": {
    "type": "table-number",
    "polling-time": -1
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 1000,
    "storage": ""
  }
}`
)
