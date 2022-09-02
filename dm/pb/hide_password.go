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
	"github.com/pingcap/tiflow/pkg/logutil"
	"go.uber.org/zap/zapcore"
)

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *StartTaskRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("HidePasswordObject", logutil.HideSensitive(m.String()))
	return nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (m *OperateSourceRequest) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("HidePasswordObject", logutil.HideSensitive(m.String()))
	return nil
}
