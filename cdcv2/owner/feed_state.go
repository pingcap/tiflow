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

import (
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdcv2/metadata"
)

// dbChangefeedState changefeed state is the underlying storage.
type dbChangefeedState struct {
	uuid             metadata.ChangefeedUUID
	id               model.ChangeFeedID
	info             *model.ChangeFeedInfo
	status           *model.ChangeFeedStatus
	ownerObservation metadata.OwnerObservation
	querier          metadata.Querier
	ca               metadata.CaptureObservation
}

// NewDBChangefeedState returns a new dbChangefeedState.
func NewDBChangefeedState(
	uuid metadata.ChangefeedUUID,
	id model.ChangeFeedID,
	info *model.ChangeFeedInfo,
	status *model.ChangeFeedStatus,
	ownerObservation metadata.OwnerObservation,
	querier metadata.Querier) *dbChangefeedState {
	return &dbChangefeedState{
		uuid:             uuid,
		id:               id,
		info:             info,
		status:           status,
		ownerObservation: ownerObservation,
		querier:          querier,
	}
}

// GetID returns the changefeed ID.
func (d *dbChangefeedState) GetID() model.ChangeFeedID {
	return d.id
}

// GetChangefeedInfo returns the changefeed info.
func (d *dbChangefeedState) GetChangefeedInfo() *model.ChangeFeedInfo {
	return d.info
}

// GetChangefeedStatus returns the changefeed status.
func (d *dbChangefeedState) GetChangefeedStatus() *model.ChangeFeedStatus {
	return d.status
}

// RemoveChangefeed removes the changefeed and clean the information and status.
func (d *dbChangefeedState) RemoveChangefeed() {
	_ = d.ownerObservation.SetChangefeedRemoved()
}

// ResumeChnagefeed resumes the changefeed and set the checkpoint ts.
func (d *dbChangefeedState) ResumeChnagefeed(uint64) {
	_ = d.ownerObservation.ResumeChangefeed()
}

// SetWarning sets the warning to changefeed
func (d *dbChangefeedState) SetWarning(err *model.RunningError) {
	_ = d.ownerObservation.SetChangefeedWarning(err)
}

// TakeProcessorWarnings reuturns the warning of the changefeed and clean the warning.
func (d *dbChangefeedState) TakeProcessorWarnings() []*model.RunningError {
	fs, err := d.querier.GetChangefeedState(d.uuid)
	if err != nil || len(fs) == 0 {
		return nil
	}
	return []*model.RunningError{fs[0].Warning}
}

// SetError sets the error to changefeed
func (d *dbChangefeedState) SetError(err *model.RunningError) {
	_ = d.ownerObservation.SetChangefeedWarning(err)
}

// TakeProcessorErrors reuturns the error of the changefeed and clean the error.
func (d *dbChangefeedState) TakeProcessorErrors() []*model.RunningError {
	fs, err := d.querier.GetChangefeedState(d.uuid)
	if err != nil || len(fs) == 0 {
		return nil
	}
	return []*model.RunningError{fs[0].Warning}
}

// CleanUpTaskPositions removes the task positions of the changefeed.
func (d *dbChangefeedState) CleanUpTaskPositions() {
	// nothing to do
}

// UpdateChangefeedState returns the task status of the changefeed.
func (d *dbChangefeedState) UpdateChangefeedState(state model.FeedState,
	adminJobType model.AdminJobType, epoch uint64) {

}
