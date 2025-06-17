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

// NewUnlockDDLLockCmd creates a UnlockDDLLock command.
func NewUnlockDDLLockCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "unlock-ddl-lock <lock-ID>",
		Short:  "Unlocks DDL lock forcefully",
		Hidden: true,
		RunE:   unlockDDLLockFunc,
	}
	cmd.Flags().StringP("owner", "o", "", "source to replace the default owner")
	cmd.Flags().BoolP("force-remove", "f", false, "force to remove DDL lock")
	cmd.Flags().StringP("action", "a", "skip", "accept skip/exec values which means whether to skip or execute ddls")
	cmd.Flags().StringP("database", "d", "", "database name of the table")
	cmd.Flags().StringP("table", "t", "", "table name")
	return cmd
}

// unlockDDLLockFunc does unlock DDL lock.
func unlockDDLLockFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	owner, err := cmd.Flags().GetString("owner")
	if err != nil {
		common.PrintLinesf("error in parse `--owner`")
		return err
	}

	lockID := cmd.Flags().Arg(0)

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	forceRemove, err := cmd.Flags().GetBool("force-remove")
	if err != nil {
		return err
	}

	database, err := cmd.Flags().GetString("database")
	if err != nil {
		return err
	}

	table, err := cmd.Flags().GetString("table")
	if err != nil {
		return err
	}

	action, err := cmd.Flags().GetString("action")
	if err != nil {
		return err
	}

	var op pb.UnlockDDLLockOp
	switch action {
	case "exec":
		op = pb.UnlockDDLLockOp_ExecLock
	case "skip":
		op = pb.UnlockDDLLockOp_SkipLock
	default:
		return errors.New("please check --action argument, only exec/skip are acceptable")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.UnlockDDLLockResponse{}
	err = common.SendRequest(
		ctx,
		"UnlockDDLLock",
		&pb.UnlockDDLLockRequest{
			ID:           lockID,
			ReplaceOwner: owner,
			ForceRemove:  forceRemove,
			Sources:      sources,
			Database:     database,
			Table:        table,
			Op:           op,
		},
		&resp,
	)
	if err != nil {
		common.PrintLinesf("can not unlock DDL lock %s", lockID)
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
