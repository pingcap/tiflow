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

package pb

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tiflow/dm/pkg/redact"
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap/zapcore"
)

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *StartTaskRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	clone := *m
	clone.Task = redact.YAMLConfigWithRedactedURL(m.Task)
	enc.AddString("HidePasswordObject", logutil.HideSensitive(clone.String()))
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *CheckTaskRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	clone := *m
	clone.Task = redact.YAMLConfigWithRedactedURL(m.Task)
	enc.AddString("HidePasswordObject", logutil.HideSensitive(clone.String()))
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *UpdateTaskRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	clone := *m
	clone.Task = redact.YAMLConfigWithRedactedURL(m.Task)
	enc.AddString("HidePasswordObject", logutil.HideSensitive(clone.String()))
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *CheckSubtasksCanUpdateRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	clone := *m
	clone.SubtaskCfgTomlString = redact.TOMLConfigWithRedactedURL(m.SubtaskCfgTomlString)
	enc.AddString("HidePasswordObject", logutil.HideSensitive(clone.String()))
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *OperateSourceRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("HidePasswordObject", logutil.HideSensitive(m.String()))
	return nil
}

// QueryStatusWithRedactedURL masks credentials in a response copy, including
// errors received from older workers. It leaves internal status data untouched.
func QueryStatusWithRedactedURL(response *QueryStatusResponse) *QueryStatusResponse {
	if response == nil {
		return nil
	}
	result := proto.Clone(response).(*QueryStatusResponse)
	result.Msg = redact.TextWithRedactedURL(result.Msg)
	cleanResult := func(r *ProcessResult) {
		if r == nil {
			return
		}
		for _, err := range r.Errors {
			if err != nil {
				err.Message = redact.TextWithRedactedURL(err.Message)
				err.RawCause = redact.TextWithRedactedURL(err.RawCause)
				err.Workaround = redact.TextWithRedactedURL(err.Workaround)
			}
		}
	}
	cleanDDLs := func(ddls []string) {
		for i := range ddls {
			ddls[i] = redact.TextWithRedactedURL(ddls[i])
		}
	}
	if source := result.SourceStatus; source != nil {
		cleanResult(source.Result)
		if source.RelayStatus != nil {
			cleanResult(source.RelayStatus.Result)
		}
	}
	for _, task := range result.SubTaskStatus {
		if task == nil {
			continue
		}
		cleanResult(task.Result)
		if task.Validation != nil {
			cleanResult(task.Validation.Result)
		}
		if sync := task.GetSync(); sync != nil {
			sync.ConflictMsg = redact.TextWithRedactedURL(sync.ConflictMsg)
			cleanDDLs(sync.BlockingDDLs)
			for _, group := range sync.UnresolvedGroups {
				if group != nil {
					cleanDDLs(group.DDLs)
				}
			}
		}
	}
	return result
}
