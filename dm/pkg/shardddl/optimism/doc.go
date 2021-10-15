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

// this pkg is used to support shard DDL in optimistic mode.
// for the basic mechanism to support shard DDL,
// please see pkg/shardddl/pessimism/doc.go.
//
// some highlight differences with pessimistic mode:
// - only one level of shard group exists, it includes all upstream tables from all replica sources.
//   and these tables are all handled in DM-master uniformly (no shard group handled in DM-worker).
// - no sequence order of DDLs from different tables required,
//   but schema versions after applied these DDLs from tables must be compatible now.
// - one shard DDL lock often constructed from multiple DDLs or schema versions,
//   so one table _done_ a lock operation doesn't mean the table has done that part of the lock,
//   and we call the lock _done_ operations until all tables reach the same schema & _done_ the last operation.
//
// some functions are not supported now, but need to be supported later.
// - handle some types of conflicts automatically.
// - revert tracked schema if fail to handle the shard DDL.
// - all shard tables always get the initial downstream schema (not the updated one by DDL from another table).
