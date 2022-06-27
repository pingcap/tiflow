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
    "protocol": "open-protocol"
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
    },
    "error-output": "stderr"
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
    "region-scan-limit": 40,
    "region-retry-duration": 60000000000
  },
  "debug": {
    "table-actor": {
      "event-batch-size": 32
    },
    "enable-db-sorter": true,
    "db": {
      "count": 8,
      "concurrency": 128,
      "max-open-files": 10000,
      "block-size": 65536,
      "block-cache-size": 4294967296,
      "writer-buffer-size": 8388608,
      "compression": "snappy",
      "target-file-size-base": 8388608,
      "write-l0-slowdown-trigger": 2147483647,
      "write-l0-pause-trigger": 2147483647,
      "compaction-l0-trigger": 160,
      "compaction-deletion-threshold": 10485760,
      "compaction-period": 1800,
      "iterator-max-alive-duration": 10000,
      "iterator-slow-read-duration": 256
    },
    "enable-new-scheduler": true,
    "messages": {
      "client-max-batch-interval": 10000000,
      "client-max-batch-size": 8388608,
      "client-max-batch-count": 128,
      "client-retry-rate-limit": 1,
      "server-max-pending-message-count": 102400,
      "server-ack-interval": 100000000,
      "server-worker-pool-size": 4
    },
    "enable-2phase-scheduler": false,
    "scheduler": {
      "heartbeat-tick": 2,
      "max-task-concurrency": 10,
      "check-balance-interval": 60000000000
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
    "protocol": "open-protocol",
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
    ],
    "schema-registry": ""
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
    "protocol": "open-protocol",
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
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 1000,
    "storage": ""
  }
}`
)
