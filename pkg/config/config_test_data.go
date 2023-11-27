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
  "memory-quota": 1073741824,
  "case-sensitive": false,
  "enable-old-value": true,
  "force-replicate": true,
  "check-gc-safe-point": true,
  "enable-sync-point": false,
  "sync-point-interval": 600000000000,
  "sync-point-retention": 86400000000000,
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
    "encoder-concurrency": 16,
    "terminator": "\r\n",
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
    "enable-partition-separator": true,
    "protocol": "canal-json",
    "advance-timeout-in-sec": 150
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 2000,
    "meta-flush-interval": 200,
    "encoding-worker-num": 16,
    "flush-worker-num": 8,
    "storage": "",
    "use-file-backend": false
  },
  "changefeed-error-stuck-duration": 1800000000000,
  "sql-mode":"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
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
  "owner-flush-interval": 50000000,
  "processor-flush-interval": 50000000,
  "sorter": {
    "sort-dir": "/tmp/sorter",
    "cache-size-in-mb": 128,
    "max-memory-percentage": 10,
    "num-concurrent-worker": 4,
    "chunk-size-limit": 999,
    "max-memory-consumption": 17179869184,
    "num-workerpool-goroutine": 16
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
    "enable-pull-based-sink": true,
    "enable-db-sorter": true,
    "db": {
      "count": 8,
      "concurrency": 128,
      "max-open-files": 10000,
      "block-size": 65536,
      "writer-buffer-size": 8388608,
      "compression": "snappy",
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
      "server-worker-pool-size": 4,
      "max-recv-msg-size": 268435456,
      "keep-alive-time": 30000000000,
      "keep-alive-timeout": 10000000000
    },
    "scheduler": {
      "heartbeat-tick": 2,
      "collect-stats-tick": 200,
      "max-task-concurrency": 10,
      "check-balance-interval": 60000000000,
      "add-table-batch-size": 50
    },
    "enable-new-sink": true,
    "enable-kv-connect-backoff": false
  },
  "cluster-id": "default",
  "max-memory-percentage": 0,
  "gc-tuner-memory-threshold": 0
}`

	testCfgTestReplicaConfigMarshal1 = `{
  "memory-quota": 1073741824,
  "case-sensitive": false,
  "enable-old-value": true,
  "force-replicate": true,
  "check-gc-safe-point": true,
  "enable-sync-point": false,
  "bdr-mode": false,
  "sync-point-interval": 600000000000,
  "sync-point-retention": 86400000000000,
  "filter": {
    "rules": [
      "1.1"
    ],
    "ignore-txn-start-ts": null,
    "event-filters": null
  },
  "mounter": {
    "worker-num": 3
  },
  "sink": {
  	"encoder-concurrency": 16,
    "dispatchers": null,
    "protocol": "canal-json",
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
    "schema-registry": "",
    "csv": {
      "delimiter": ",",
      "quote": "\"",
      "null": "\\N",
      "include-commit-ts": true,
      "binary-encoding-method":"base64"
    },
    "transaction-atomicity": "",
    "terminator": "",
    "date-separator": "month",
    "enable-partition-separator": true,
    "kafka-config": {
      "large-message-handle": {
        "large-message-handle-option": "handle-key-only"
      }
    },
    "advance-timeout-in-sec": 150
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 2000,
    "meta-flush-interval": 200,
    "encoding-worker-num": 16,
    "flush-worker-num": 8,
    "storage": "",
    "use-file-backend": false
  },
  "changefeed-error-stuck-duration": 1800000000000,
  "sql-mode":"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
}`

	testCfgTestReplicaConfigMarshal2 = `{
  "memory-quota": 1073741824,
  "case-sensitive": false,
  "enable-old-value": true,
  "force-replicate": true,
  "check-gc-safe-point": true,
  "enable-sync-point": false,
  "bdr-mode": false,
  "sync-point-interval": 600000000000,
  "sync-point-retention": 86400000000000,
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
    "encoder-concurrency": 16,
    "dispatchers": null,
    "protocol": "canal-json",
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
    "csv": {
      "delimiter": ",",
      "quote": "\"",
      "null": "\\N",
      "include-commit-ts": true,
      "binary-encoding-method":"base64"
    },
    "terminator": "",
    "date-separator": "month",
    "enable-partition-separator": true,
    "kafka-config": {
      "large-message-handle": {
        "large-message-handle-option": "handle-key-only"
      }
    },
    "advance-timeout-in-sec": 150
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 2000,
    "meta-flush-interval": 200,
    "encoding-worker-num": 16,
    "flush-worker-num": 8,
    "storage": "",
    "use-file-backend": false
  },
  "changefeed-error-stuck-duration": 1800000000000,
  "sql-mode":"ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"
}`
)
