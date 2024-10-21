// Copyright 2021 PingCAP, Inc.
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

// NewTransferSourceCmd creates a TransferSource command.
func NewTransferSourceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "transfer-source <source-id> <worker-id>",
		Short: "Transfers a upstream MySQL/MariaDB source to a free worker",
		RunE:  transferSourceFunc,
	}
	return cmd
}

func transferSourceFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) != 2 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	sourceID := cmd.Flags().Arg(0)
	workerID := cmd.Flags().Arg(1)
	var err error

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.TransferSourceResponse{}
	err = common.SendRequest(
		ctx,
		"TransferSource",
		&pb.TransferSourceRequest{
			Source: sourceID,
			Worker: workerID,
		},
		&resp,
	)
	if err != nil {
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
