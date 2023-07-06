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

package model

import (
	"errors"
	"time"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// RunningError represents some running error from cdc components, such as processor.
type RunningError struct {
	Time    time.Time `json:"time"`
	Addr    string    `json:"addr"`
	Code    string    `json:"code"`
	Message string    `json:"message"`
}

// // 发生时间，发生地址，发生组件，错误码，错误信息
// // 重试时间，发生时 CheckpointTs
// func NewRunningError(err error) *RunningError {
// 	return &RunningError{
// 		Time:    time.Now(),
// 		Code:    cerror.RFCCode(err),
// 		Message: err.Error(),
// 	}
// }

// IsChangefeedUnRetryableError return true if a running error contains a changefeed not retry error.
func (r RunningError) IsChangefeedUnRetryableError() bool {
	return cerror.IsChangefeedUnRetryableError(errors.New(r.Message + r.Code))
}
