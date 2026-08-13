// Copyright 2019 PingCAP, Inc.
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

package checker

import (
	"context"
	"fmt"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/checker"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

var (
	// CheckTaskMsgHeader used as the header of the error/warning message when checking config failed.
	CheckTaskMsgHeader = "fail to check synchronization configuration with type"

	CheckTaskSuccess = "pre-check is passed. "

	// CheckSyncConfigFunc holds the CheckSyncConfig function.
	CheckSyncConfigFunc func(ctx context.Context, cfgs []*config.SubTaskConfig, errCnt, warnCnt int64) (string, error)
)

func init() {
	CheckSyncConfigFunc = CheckSyncConfig
}

// CheckSyncConfig checks synchronization configuration.
func CheckSyncConfig(ctx context.Context, cfgs []*config.SubTaskConfig, errCnt, warnCnt int64) (string, error) {
	if len(cfgs) == 0 {
		return "", nil
	}

	// all `IgnoreCheckingItems` and `Mode` of sub-task are same, so we take first one
	// for ModeFull we don't need replication privilege; for ModeIncrement we don't need dump privilege
	ignoreCheckingItems := cfgs[0].IgnoreCheckingItems
	checkingItems := config.FilterCheckingItems(ignoreCheckingItems)
	if len(checkingItems) == 0 {
		return "", nil
	}

	c := NewChecker(cfgs, checkingItems, errCnt, warnCnt)

	if err := c.Init(ctx); err != nil {
		return "", terror.Annotate(err, "fail to initialize checker")
	}
	defer c.Close()

	pr := make(chan pb.ProcessResult, 1)
	c.Process(ctx, pr)
	if len(pr) > 0 {
		r := <-pr
		// we only want first error
		if len(r.Errors) > 0 {
			return "", terror.ErrTaskCheckSyncConfigError.Generate(CheckTaskMsgHeader, r.Errors[0].Message, string(r.Detail))
		}
		if len(r.Detail) == 0 {
			return CheckTaskSuccess, nil
		}
		return fmt.Sprintf("%s: no errors but some warnings\n detail: %s", CheckTaskMsgHeader, string(r.Detail)), nil
	}

	return "", nil
}

func runCheckOnConfigs(
	ctx context.Context,
	cfgs []*config.SubTaskConfig,
	dumpWholeInstance bool,
	extraCheckingItems ...string,
) (*checker.Results, error) {
	if len(cfgs) == 0 {
		return nil, nil
	}

	ignoreCheckingItems := cfgs[0].IgnoreCheckingItems
	checkingItems := config.FilterCheckingItems(ignoreCheckingItems)
	// keep backward compatibility: if no checking items selected from config
	// and no extra checking items provided, return nil
	if len(checkingItems) == 0 && len(extraCheckingItems) == 0 {
		return nil, nil
	}

	// merge extraCheckingItems (caller-specified) into checkingItems
	for _, it := range extraCheckingItems {
		if it == config.AllChecking {
			// if caller explicitly requests 'all', keep original behavior and
			// treat as no-op here (all checks are controlled by config.FilterCheckingItems)
			continue
		}
		if _, ok := checkingItems[it]; !ok {
			// try to get description from config.AllCheckingItems first, then AdditionalCheckingItems
			if desc, ok2 := config.AllCheckingItems[it]; ok2 {
				checkingItems[it] = desc
			} else if desc2, ok3 := config.AdditionalCheckingItems[it]; ok3 {
				checkingItems[it] = desc2
			} else {
				checkingItems[it] = ""
			}
		}
	}

	c := NewChecker(cfgs, checkingItems, 0, 0)
	c.dumpWholeInstance = dumpWholeInstance

	if err := c.Init(ctx); err != nil {
		return nil, terror.Annotate(err, "fail to initialize checker")
	}
	defer c.Close()

	return checker.Do(ctx, c.checkList)
}

// RunCheckOnConfigs returns the check result for given subtask configs. Caller
// should be noticed that result may be very large. The result will be truncated
// to `warnLimit` and `errLimit`, but the total count in summary will be the same
// as the original result.
//
// when `dumpWholeInstance` is true, checker will require SELECT ON *.*
// privileges for SourceDumpPrivilegeChecker.
//
// This function is used by cloud services.
func RunCheckOnConfigs(
	ctx context.Context,
	cfgs []*config.SubTaskConfig,
	dumpWholeInstance bool,
	warnLimit, errLimit int64,
	extraCheckingItems ...string,
) (*checker.Results, error) {
	result, err := runCheckOnConfigs(ctx, cfgs, dumpWholeInstance, extraCheckingItems...)
	if err != nil || result == nil {
		return result, err
	}

	filterResults(result, warnLimit, errLimit, true)
	return result, nil
}
