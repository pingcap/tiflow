// Copyright 2023 PingCAP, Inc.
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

package owner

import "github.com/pingcap/tiflow/cdc/model"

// ChangefeedState is the interface for changefeed state in underlying storage.
type ChangefeedState interface {
	// GetID returns the changefeed ID.
	GetID() model.ChangeFeedID
	// GetChangefeedInfo returns the changefeed info.
	GetChangefeedInfo() *model.ChangeFeedInfo
	// GetChangefeedStatus returns the changefeed status.
	GetChangefeedStatus() *model.ChangeFeedStatus
	// RemoveChangefeed removes the changefeed and clean the information and status.
	RemoveChangefeed()
	// ResumeChangefeed resumes the changefeed and set the checkpoint ts.
	ResumeChangefeed(uint64)
	// SetWarning sets the warning to changefeed
	SetWarning(*model.RunningError)
	// TakeProcessorWarnings reuturns the warning of the changefeed and clean the warning.
	TakeProcessorWarnings() []*model.RunningError
	// SetError sets the error to changefeed
	SetError(*model.RunningError)
	// TakeProcessorErrors reuturns the error of the changefeed and clean the error.
	TakeProcessorErrors() []*model.RunningError
	// CleanUpTaskPositions removes the task positions of the changefeed.
	CleanUpTaskPositions()
	// UpdateChangefeedState returns the task status of the changefeed.
	UpdateChangefeedState(model.FeedState, model.AdminJobType, uint64)
}
