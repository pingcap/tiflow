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

package master

import (
	"fmt"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// NewShowDDLLocksCmd creates a ShowDDlLocks command
func NewShowDDLLocksCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show-ddl-locks [-w worker ...] [task_name]",
		Short: "show un-resolved DDL locks",
		Run:   showDDLLocksFunc,
	}
	return cmd
}

// showDDLLocksFunc does show DDL locks
func showDDLLocksFunc(cmd *cobra.Command, _ []string) {
	if len(cmd.Flags().Args()) > 1 {
		fmt.Println(cmd.Usage())
		return
	}
	taskName := cmd.Flags().Arg(0) // maybe empty

	workers, err := common.GetWorkerArgs(cmd)
	if err != nil {
		common.PrintLines("%s", errors.ErrorStack(err))
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli := common.MasterClient()
	resp, err := cli.ShowDDLLocks(ctx, &pb.ShowDDLLocksRequest{
		Task:    taskName,
		Workers: workers,
	})
	if err != nil {
		common.PrintLines("can not show DDL locks for task %s and workers %v:\n%s", taskName, workers, errors.ErrorStack(err))
		return
	}

	common.PrettyPrintResponse(resp)
}
