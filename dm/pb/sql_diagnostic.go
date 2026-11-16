// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pb

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

// SQLDiagnosticCopy returns a diagnostic copy with SQL literals hidden.
// Execution errors, retry state and stored shard DDLs must use the original.
func (m *QueryStatusResponse) SQLDiagnosticCopy() *QueryStatusResponse {
	if m == nil || !log.IsRedactLogEnabled() {
		return m
	}
	clone := proto.Clone(m).(*QueryStatusResponse)
	if source := clone.SourceStatus; source != nil {
		redactProcessSQL(source.Result)
		if source.RelayStatus != nil {
			redactProcessSQL(source.RelayStatus.Result)
		}
	}
	for _, status := range clone.SubTaskStatus {
		redactSubTaskSQL(status)
	}
	return clone
}

// SQLDiagnosticCopy returns a copy for status APIs and status logs.
func (m *SubTaskStatus) SQLDiagnosticCopy() *SubTaskStatus {
	if m == nil || !log.IsRedactLogEnabled() {
		return m
	}
	clone := proto.Clone(m).(*SubTaskStatus)
	redactSubTaskSQL(clone)
	return clone
}

// SQLDiagnosticCopy preserves progress and groups while hiding DDL literals.
func (m *SyncStatus) SQLDiagnosticCopy() *SyncStatus {
	if m == nil || !log.IsRedactLogEnabled() {
		return m
	}
	clone := proto.Clone(m).(*SyncStatus)
	redactSyncSQL(clone)
	return clone
}

func redactProcessSQL(result *ProcessResult) {
	if result == nil {
		return
	}
	for _, err := range result.Errors {
		if err != nil {
			err.Message, err.RawCause = log.RedactSQLDiagnostic(err.ErrCode, err.Message, err.RawCause)
		}
	}
}

func redactSubTaskSQL(status *SubTaskStatus) {
	if status == nil {
		return
	}
	redactProcessSQL(status.Result)
	if status.Validation != nil {
		redactProcessSQL(status.Validation.Result)
	}
	if sync := status.GetSync(); sync != nil {
		redactSyncSQL(sync)
	}
}

func redactSyncSQL(status *SyncStatus) {
	status.BlockingDDLs = log.RedactSQLs(status.BlockingDDLs)
	for _, group := range status.UnresolvedGroups {
		if group != nil {
			group.DDLs = log.RedactSQLs(group.DDLs)
		}
	}
	// ConflictMsg is free-form schema-comparison text, not necessarily SQL.
	// It needs its own policy rather than feeding arbitrary text to a SQL parser.
}
