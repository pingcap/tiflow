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

package openapi

// JobAPIPrefix is the prefix of the job API.
const JobAPIPrefix = "/api/v1/jobs/"

// JobDetailAPIFormat is the path format for job detail status
// the entire path is: /api/v1/jobs/${jobID}/status
const JobDetailAPIFormat = JobAPIPrefix + "%s/status"
