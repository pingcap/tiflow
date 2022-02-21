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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/tiflow/dm/dm/master/scheduler"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

const (
	checkMemberTimes    = 5
	checkMemberInterval = 10 * time.Second
)

// checkMembersReadyLoop checks whether all DM-master and DM-worker members have been ready.
// NOTE: in this chaos case, we ensure 3 DM-master and 3 DM-worker started.
func checkMembersReadyLoop(ctx context.Context, cli pb.MasterClient, masterCount, workerCount int) (err error) {
	for i := 0; i < checkMemberTimes; i++ {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(checkMemberInterval):
			err = checkMembersReady(ctx, cli, masterCount, workerCount)
			if err == nil {
				return nil
			}
		}
	}
	return err
}

func checkMembersReady(ctx context.Context, cli pb.MasterClient, masterCount, workerCount int) error {
	resp, err := cli.ListMember(ctx, &pb.ListMemberRequest{})
	if err != nil {
		return err
	} else if !resp.Result {
		return fmt.Errorf("fail to list member: %s", resp.Msg)
	}

	var (
		hasLeader       bool
		allMasterAlive  bool
		allWorkerOnline bool
	)

	for _, m := range resp.Members {
		if m.GetLeader() != nil {
			hasLeader = true
		} else if lm := m.GetMaster(); lm != nil {
			var aliveCount int
			for _, master := range lm.Masters {
				if master.Alive {
					aliveCount++
				}
			}
			allMasterAlive = aliveCount == masterCount
		} else if lw := m.GetWorker(); lw != nil {
			var onlineCount int
			for _, worker := range lw.Workers {
				if worker.Stage != string(scheduler.WorkerOffline) {
					onlineCount++
				}
			}
			allWorkerOnline = onlineCount == workerCount
		}
	}

	if !hasLeader || !allMasterAlive || !allWorkerOnline {
		return fmt.Errorf("not all members are ready: %s", resp.String())
	}
	return nil
}
