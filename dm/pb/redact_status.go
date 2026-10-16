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

import "github.com/gogo/protobuf/proto"

// RedactProcessResult returns a diagnostic copy, preserving error codes and
// cancellation state. Internal retry classification must use the original.
func RedactProcessResult(result *ProcessResult) *ProcessResult {
	if result == nil {
		return nil
	}
	clone := proto.Clone(result).(*ProcessResult)
	if len(clone.Detail) > 0 {
		clone.Detail = []byte("?")
	}
	for _, err := range clone.Errors {
		if err == nil {
			continue
		}
		if err.Message != "" {
			err.Message = "?"
		}
		if err.RawCause != "" {
			err.RawCause = "?"
		}
		if err.Workaround != "" {
			err.Workaround = "?"
		}
	}
	return clone
}

// RedactSubTaskStatus returns a status copy suitable for redacted diagnostics.
func RedactSubTaskStatus(status *SubTaskStatus) *SubTaskStatus {
	if status == nil {
		return nil
	}
	clone := proto.Clone(status).(*SubTaskStatus)
	clone.Result = RedactProcessResult(clone.Result)
	if clone.Validation != nil {
		clone.Validation.Result = RedactProcessResult(clone.Validation.Result)
	}
	if check := clone.GetCheck(); check != nil && len(check.Detail) > 0 {
		check.Detail = []byte("?")
	}
	if sync := clone.GetSync(); sync != nil {
		for i := range sync.BlockingDDLs {
			sync.BlockingDDLs[i] = "?"
		}
		if sync.ConflictMsg != "" {
			sync.ConflictMsg = "?"
		}
		for _, group := range sync.UnresolvedGroups {
			if group != nil {
				for i := range group.DDLs {
					group.DDLs[i] = "?"
				}
			}
		}
	}
	return clone
}

// RedactQueryStatus protects error text before sending status to the master,
// which can log the response in a different process with different settings.
func RedactQueryStatus(status *QueryStatusResponse) *QueryStatusResponse {
	clone := proto.Clone(status).(*QueryStatusResponse)
	if source := clone.SourceStatus; source != nil {
		source.Result = RedactProcessResult(source.Result)
		if source.RelayStatus != nil {
			source.RelayStatus.Result = RedactProcessResult(source.RelayStatus.Result)
		}
	}
	for i, subtask := range clone.SubTaskStatus {
		clone.SubTaskStatus[i] = RedactSubTaskStatus(subtask)
	}
	return clone
}
