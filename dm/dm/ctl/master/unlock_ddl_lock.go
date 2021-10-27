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
	"fmt"
	"os"

	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"

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

	sources, _ := common.GetSourceArgs(cmd)
	if len(sources) > 0 {
		fmt.Println("should not specify any sources")
		return errors.New("please check output to see error")
	}

	forceRemove, err := cmd.Flags().GetBool("force-remove")
	if err != nil {
		return err
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
