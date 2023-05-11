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

// RunningError represents some running errors from cdc components, such as processor.
type RunningError struct {
	Time    time.Time `json:"time"`
	Addr    string    `json:"addr"`
	Code    string    `json:"code"`
	Message string    `json:"message"`
}

// IsChangefeedUnRetryableError return true if a running error contains a changefeed not retry error.
func (r RunningError) IsChangefeedUnRetryableError() bool {
	return cerror.IsChangefeedUnRetryableError(errors.New(r.Message + r.Code))
}

const (
	// ComponentProcessorSink indicates the sink module in processor.
	ComponentProcessorSink string = "processor/sink"
	// ComponentOwnerSink indicates the sink module in owner.
	ComponentOwnerSink string = "owner/sink"
)

// Warning is like an error, but has one difference:
// generally an error will stop and restart a changefeed, but a warning won't.
type Warning struct {
	err       error
	Component string
}

// Error implements builtin `error` interface.
func (e Warning) Error() string {
	return e.err.Error()
}

// NewWarning creates a Warning.
func NewWarning(e error, component string) Warning {
	return Warning{e, component}
}
