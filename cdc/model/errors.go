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
	"fmt"
	"time"

	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type ComponentType int

const (
	Processor ComponentType = iota + 1
	ProcessorSink
	ProcessorRedo

	Owner
	OwnerDDLSink
)

// RunningError represents some running error from cdc components, such as processor.
type RunningError struct {
	Time      time.Time `json:"time"`
	Addr      string    `json:"addr"`
	Code      string    `json:"code"`
	Message   string    `json:"message"`
	Component int       `json:"component"`
}

// IsChangefeedUnRetryableError return true if a running error contains a changefeed not retry error.
func (r RunningError) IsChangefeedUnRetryableError() bool {
	return cerror.IsChangefeedUnRetryableError(errors.New(r.Message + r.Code))
}

type ComponentError struct {
	error
	Component ComponentType
}

func (e ComponentError) Error() string {
	var comp string
	switch e.Component {
	case Processor:
		comp = "Processor"
	case ProcessorSink:
		comp = "ProcessorSink"
	case ProcessorRedo:
		comp = "ProcessorRedo"
	case Owner:
		comp = "Owner"
	case OwnerDDLSink:
		comp = "OwnerDDLSink"
	}
	return fmt.Sprintf("%s(component=%s)", e.error.Error(), comp)
}

func NewComponentError(e error, component ComponentType) ComponentError {
	return ComponentError{e, component}
}
