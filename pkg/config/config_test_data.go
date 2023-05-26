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
<<<<<<< HEAD
=======
  "enable-sync-point": false,
  "bdr-mode": false,
  "sync-point-interval": 600000000000,
  "sync-point-retention": 86400000000000,
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
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
    "encoder-concurrency": 16,
<<<<<<< HEAD
=======
    "terminator": "\r\n",
	"date-separator": "none",
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
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
<<<<<<< HEAD
    "protocol": "open-protocol"
=======
    "enable-partition-separator": true,
    "protocol": "open-protocol",
	"enable-kafka-sink-v2": false,
	"only-output-updated-columns": false
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
  },
  "cyclic-replication": {
    "enable": false,
    "replica-id": 0,
    "filter-replica-ids": null,
    "id-buckets": 0,
    "sync-ddl": false
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 2000,
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
    "enable-table-actor": false,
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
      "server-worker-pool-size": 4,
      "max-recv-msg-size": 268435456
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
<<<<<<< HEAD
    "transaction-atomicity": "",
=======
  	"encoder-concurrency": 16,
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
    "protocol": "open-protocol",
    "dispatchers": null,
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
<<<<<<< HEAD
    "schema-registry": "",
    "encoder-concurrency": 16
  },
  "cyclic-replication": {
    "enable": false,
    "replica-id": 0,
    "filter-replica-ids": null,
    "id-buckets": 0,
    "sync-ddl": false
=======
    "csv": {
      "delimiter": ",",
      "quote": "\"",
      "null": "\\N",
      "include-commit-ts": true
    },
    "date-separator": "month",
    "enable-partition-separator": true,
    "only-output-updated-columns": false,
    "enable-kafka-sink-v2": true,
    "only-output-updated-columns": true,
    "safe-mode": true,
	"terminator": "\r\n",
	"transaction-atomicity": "",
    "kafka-config": {
      "partition-num": 1,
      "replication-factor": 1,
      "kafka-version": "version",
      "max-message-bytes": 1,
      "compression": "gzip",
      "kafka-client-id": "client-id",
      "auto-create-topic": true,
      "dial-timeout": "1m",
      "write-timeout": "1m",
      "read-timeout": "1m",
      "required-acks": 1,
      "sasl-user": "user",
      "sasl-password": "password",
      "sasl-mechanism": "mechanism",
      "sasl-gssapi-auth-type": "type",
      "sasl-gssapi-keytab-path": "path",
      "sasl-gssapi-kerberos-config-path": "path",
      "sasl-gssapi-service-name": "service",
      "sasl-gssapi-user": "user",
      "sasl-gssapi-password": "password",
      "sasl-gssapi-realm": "realm",
      "sasl-gssapi-disable-pafxfast": true,
      "enable-tls": true,
      "ca": "ca",
      "cert": "cert",
      "key": "key",
      "codec-config": {
        "enable-tidb-extension": true,
        "max-batch-size": 100000,
        "avro-enable-watermark": true,
        "avro-decimal-handling-mode": "string",
        "avro-bigint-unsigned-handling-mode": "string"
      }
    },
    "mysql-config": {
      "worker-count": 8,
      "max-txn-row": 100000,
      "max-multi-update-row-size": 100000,
      "max-multi-update-row": 100000,
      "tidb-txn-mode": "pessimistic",
      "ssl-ca": "ca",
      "ssl-cert": "cert",
      "ssl-key": "key",
      "time-zone": "UTC",
      "write-timeout": "1m",
      "read-timeout": "1m",
      "timeout": "1m",
      "enable-batch-dml": true,
      "enable-multi-statement": true,
      "enable-cache-prepared-statement": true
    },
    "cloud-storage-config": {
      "worker-count": 8,
      "flush-interval": "1m",
      "file-size": 1024
    }
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 2000,
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
    "encoder-concurrency": 16,
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
<<<<<<< HEAD
    ]
  },
  "cyclic-replication": {
    "enable": false,
    "replica-id": 0,
    "filter-replica-ids": null,
    "id-buckets": 0,
    "sync-ddl": false
=======
    ],
    "csv": {
      "delimiter": ",",
      "quote": "\"",
      "null": "\\N",
      "include-commit-ts": true
    },
    "terminator": "\r\n",
	"transaction-atomicity": "",
    "date-separator": "month",
    "enable-partition-separator": true,
    "only-output-updated-columns": false,
	"enable-kafka-sink-v2": true,
    "only-output-updated-columns": true,
    "safe-mode": true,
    "kafka-config": {
      "partition-num": 1,
      "replication-factor": 1,
      "kafka-version": "version",
      "max-message-bytes": 1,
      "compression": "gzip",
      "kafka-client-id": "client-id",
      "auto-create-topic": true,
      "dial-timeout": "1m",
      "write-timeout": "1m",
      "read-timeout": "1m",
      "required-acks": 1,
      "sasl-user": "user",
      "sasl-password": "password",
      "sasl-mechanism": "mechanism",
      "sasl-gssapi-auth-type": "type",
      "sasl-gssapi-keytab-path": "path",
      "sasl-gssapi-kerberos-config-path": "path",
      "sasl-gssapi-service-name": "service",
      "sasl-gssapi-user": "user",
      "sasl-gssapi-password": "password",
      "sasl-gssapi-realm": "realm",
      "sasl-gssapi-disable-pafxfast": true,
      "enable-tls": true,
      "ca": "ca",
      "cert": "cert",
      "key": "key",
      "codec-config": {
        "enable-tidb-extension": true,
        "max-batch-size": 100000,
        "avro-enable-watermark": true,
        "avro-decimal-handling-mode": "string",
        "avro-bigint-unsigned-handling-mode": "string"
      }
    },
    "mysql-config": {
      "worker-count": 8,
      "max-txn-row": 100000,
      "max-multi-update-row-size": 100000,
      "max-multi-update-row": 100000,
      "tidb-txn-mode": "pessimistic",
      "ssl-ca": "ca",
      "ssl-cert": "cert",
      "ssl-key": "key",
      "time-zone": "UTC",
      "write-timeout": "1m",
      "read-timeout": "1m",
      "timeout": "1m",
      "enable-batch-dml": true,
      "enable-multi-statement": true,
      "enable-cache-prepared-statement": true
    },
    "cloud-storage-config": {
      "worker-count": 8,
      "flush-interval": "1m",
      "file-size": 1024
    }
>>>>>>> c601a1adb6 (pkg/config(ticdc): hide fields that are not required for specific protocols (#8836))
  },
  "consistent": {
    "level": "none",
    "max-log-size": 64,
    "flush-interval": 2000,
    "storage": ""
  }
}`
)
