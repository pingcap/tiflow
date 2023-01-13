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

package executor

import (
	"sync"

	cvstask "github.com/pingcap/tiflow/engine/executor/cvs"
	dmtask "github.com/pingcap/tiflow/engine/executor/dm"
	fakejobTask "github.com/pingcap/tiflow/engine/executor/fakejob"
	cvs "github.com/pingcap/tiflow/engine/jobmaster/cvsjob"
	"github.com/pingcap/tiflow/engine/jobmaster/dm"
	"github.com/pingcap/tiflow/engine/jobmaster/fakejob"
)

// registerWorkerOnce guards calling registerWorkers.
var registerWorkerOnce sync.Once

func registerWorkers() {
	cvstask.RegisterWorker()
	cvs.RegisterWorker()
	dm.RegisterWorker()
	dmtask.RegisterWorker()
	fakejob.RegisterWorker()
	fakejobTask.RegisterWorker()
}
