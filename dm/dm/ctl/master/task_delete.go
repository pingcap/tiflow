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

package master

import (
	"bufio"
	"context"
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/openapi"
)

// NewTaskDeleteCmd creates a task delete command.
func NewTaskDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <task-name | config-file> [--force] [--yes]",
		Short: "Delete a task",
		RunE:  taskDeleteFunc,
	}
	cmd.Flags().BoolP("force", "f", false, "force to remove a task")
	cmd.Flags().BoolP("yes", "y", false, "execute delete command and skip confirm")
	return cmd
}

func taskDeleteFunc(cmd *cobra.Command, _ []string) (err error) {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
	force, err := cmd.Flags().GetBool("force")
	if err != nil {
		common.PrintLinesf("error in parse `--force`")
		return err
	}

	skipConfirm, err := cmd.Flags().GetBool("yes")
	if err != nil {
		common.PrintLinesf("error in parse `--yes`")
		return err
	}
	if !skipConfirm {
		common.PrintLinesf("Will delete task `%s`. Do you want to continue? [y/N]:(default=N)", taskName)
		isConfirm, err := confirm()
		if err != nil || !isConfirm {
			return err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// delete task
	deleteReq := &openapi.DMAPIDeleteTaskParams{
		Force: &force,
	}
	params := []interface{}{taskName, deleteReq}
	errResp := common.SendOpenapiRequest(ctx, "DMAPIDeleteTask", params, http.StatusNoContent, nil)
	if errResp != nil {
		return errResp
	}
	common.PrettyPrintOpenapiResp(true, "Delete task "+taskName+" success.")
	return nil
}

func confirm() (bool, error) {
	reader := bufio.NewReader(os.Stdin)
	input, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	input = strings.ToLower(strings.TrimSpace(strings.TrimSuffix(input, "\n")))

	if input == "y" || input == "yes" {
		return true, nil
	}
	common.PrintLinesf("Operation aborted by user (with answer '%s')", input)
	return false, nil
}
