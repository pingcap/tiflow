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

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/terror"
)

var (
	// ErrorMsgHeader used as the header of the error message when checking config failed.
	ErrorMsgHeader = "fail to check synchronization configuration with type"

	// CheckSyncConfigFunc holds the CheckSyncConfig function.
	CheckSyncConfigFunc func(ctx context.Context, cfgs []*config.SubTaskConfig, errCnt, warnCnt int64) error
)

func init() {
	CheckSyncConfigFunc = CheckSyncConfig
}

// CheckSyncConfig checks synchronization configuration.
func CheckSyncConfig(ctx context.Context, cfgs []*config.SubTaskConfig, errCnt, warnCnt int64) error {
	if len(cfgs) == 0 {
		return nil
	}

	// all `IgnoreCheckingItems` and `Mode` of sub-task are same, so we take first one
	// for ModeFull we don't need replication privilege; for ModeIncrement we don't need dump privilege
	ignoreCheckingItems := cfgs[0].IgnoreCheckingItems
	// we directly append ignore checking items here which may cause duplicate in ignoreCheckingItems
	// but in config.FilterCheckingItems we only use this to delete map's keys so it is tolerable to append directly here
	switch cfgs[0].Mode {
	case config.ModeFull:
		ignoreCheckingItems = append(ignoreCheckingItems, config.ReplicationPrivilegeChecking,
			config.BinlogEnableChecking, config.BinlogFormatChecking, config.BinlogRowImageChecking, config.ServerIDChecking)
	case config.ModeIncrement:
		ignoreCheckingItems = append(ignoreCheckingItems, config.DumpPrivilegeChecking)
	}
	checkingItems := config.FilterCheckingItems(ignoreCheckingItems)
	if len(checkingItems) == 0 {
		return nil
	}

	c := NewChecker(cfgs, checkingItems, errCnt, warnCnt)

	if err := c.Init(ctx); err != nil {
		return terror.Annotate(err, "fail to initial checker")
	}
	defer c.Close()

	pr := make(chan pb.ProcessResult, 1)
	c.Process(ctx, pr)
	for len(pr) > 0 {
		r := <-pr
		// we only want first error
		if len(r.Errors) > 0 {
			return terror.ErrTaskCheckSyncConfigError.Generate(ErrorMsgHeader, r.Errors[0].Message, string(r.Detail))
		}
	}

	return nil
}
