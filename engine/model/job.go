// Copyright 2022 PingCAP, Inc.
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

type (
	// JobID is the unique identifier of a job
	JobID = string
	// JobType is the type of a type, like `CVSDEMO`, `DM`, `CDC`.
	JobType int32
)

// Define job type
const (
	JobTypeInvalid = JobType(iota)
	// job type for job manager, only for inner use
	JobTypeJobManager
	JobTypeCVSDemo
	JobTypeDM
	JobTypeCDC
	JobTypeFakeJob
)

// Define job type name
const (
	JobTypeNameInvalid    = "Invalid"
	JobTypeNameJobManager = "JobManager"
	JobTypeNameCVSDemo    = "CVSDemo"
	JobTypeNameDM         = "DM"
	JobTypeNameCDC        = "CDC"
	JobTypeNameFakeJob    = "FakeJob"
)

// jobTypeNameToType maintains the valid job name and job type
var jobTypeNameToType = map[string]JobType{
	JobTypeNameCVSDemo: JobTypeCVSDemo,
	JobTypeNameDM:      JobTypeDM,
	JobTypeNameCDC:     JobTypeCDC,
	JobTypeNameFakeJob: JobTypeFakeJob,
}

// GetJobTypeByName get JobType by readable job name
func GetJobTypeByName(name string) (JobType, bool) {
	tp, exists := jobTypeNameToType[name]
	if !exists {
		return JobTypeInvalid, false
	}

	return tp, true
}

func (j JobType) String() string {
	switch j {
	case JobTypeCVSDemo:
		return JobTypeNameCVSDemo
	case JobTypeDM:
		return JobTypeNameDM
	case JobTypeCDC:
		return JobTypeNameCDC
	case JobTypeFakeJob:
		return JobTypeNameFakeJob
	case JobTypeJobManager:
		return JobTypeNameJobManager
	}

	return JobTypeNameInvalid
}
