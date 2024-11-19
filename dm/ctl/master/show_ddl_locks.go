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
	"context"
	"errors"
	"os"

	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/spf13/cobra"
)

// NewShowDDLLocksCmd creates a ShowDDlLocks command.
func NewShowDDLLocksCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "show-ddl-locks [-s source ...] [task-name | task-file]",
		Short:  "Shows un-resolved DDL locks",
		Hidden: true,
		RunE:   showDDLLocksFunc,
	}
	return cmd
}

// showDDLLocksFunc does show DDL locks.
func showDDLLocksFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) > 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0)) // maybe empty

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.ShowDDLLocksResponse{}
	err = common.SendRequest(
		ctx,
		"ShowDDLLocks",
		&pb.ShowDDLLocksRequest{
			Task:    taskName,
			Sources: sources,
		},
		&resp,
	)
	if err != nil {
		common.PrintLinesf("can not show DDL locks for task %s and sources %v", taskName, sources)
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
