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

package worker

import (
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/ha"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func getExpectValidatorStage(cfg config.ValidatorConfig, etcdClient *clientv3.Client, source, task string, revision int64) (pb.Stage, error) {
	// for subtask with validation mode=none, there is no validator stage, set to invalid
	expectedValidatorStage := pb.Stage_InvalidStage
	if cfg.Mode != config.ValidationNone {
		validatorStageM, _, err := ha.GetValidatorStage(etcdClient, source, task, revision)
		if err != nil {
			return expectedValidatorStage, err
		}
		if s, ok := validatorStageM[task]; ok {
			expectedValidatorStage = s.Expect
		}
	}
	return expectedValidatorStage, nil
}
