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

package master

import (
	"context"
	"errors"
	"os"

	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/spf13/cobra"
)

// NewHandleErrorCmd creates a HandleError command.
func NewHandleErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "handle-error <task-name | task-file> [-s source ...] [-b binlog-pos] <skip/replace/revert> [replace-sql1;replace-sql2;]",
		Short:  "`skip`/`replace`/`revert` the current error event or a specific binlog position (binlog-pos) event",
		Hidden: true,
		RunE:   handleErrorFunc,
	}
	cmd.Flags().StringP("binlog-pos", "b", "", "position used to match binlog event if matched the handler-error operation will be applied. The format like \"mysql-bin|000001.000003:3270\"")
	return cmd
}

func convertOp(t string) pb.ErrorOp {
	switch t {
	case "skip":
		return pb.ErrorOp_Skip
	case "replace":
		return pb.ErrorOp_Replace
	case "revert":
		return pb.ErrorOp_Revert
	default:
		return pb.ErrorOp_InvalidErrorOp
	}
}

// handleErrorFunc does handle error request.
func handleErrorFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) < 2 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	taskName := common.GetTaskNameFromArgOrFile(cmd.Flags().Arg(0))
	operation := cmd.Flags().Arg(1)
	var sqls []string
	var err error

	op := convertOp(operation)
	switch op {
	case pb.ErrorOp_Skip, pb.ErrorOp_Revert:
		if len(cmd.Flags().Args()) > 2 {
			common.PrintLinesf("replace-sqls can not be used for 'skip/revert' operation")
			return errors.New("please check output to see error")
		}
	case pb.ErrorOp_Replace:
		if len(cmd.Flags().Args()) <= 2 {
			common.PrintLinesf("must specify the replace-sqls for replace operation")
			return errors.New("please check output to see error")
		}

		sqls, err = common.ExtractSQLsFromArgs(cmd.Flags().Args()[2:])
		if err != nil {
			return err
		}
	default:
		common.PrintLinesf("invalid operation '%s', please use `skip`, `replace` or `revert`", operation)
		return errors.New("please check output to see error")
	}
	request := &pb.HandleErrorRequest{
		Op:   op,
		Task: taskName,
		Sqls: sqls,
	}
	return sendHandleErrorRequest(cmd, request)
}

func sendHandleErrorRequest(cmd *cobra.Command, request *pb.HandleErrorRequest) error {
	binlogPos, err := cmd.Flags().GetString("binlog-pos")
	if err != nil {
		return err
	}
	if len(binlogPos) != 0 {
		_, err = binlog.VerifyBinlogPos(binlogPos)
		if err != nil {
			return err
		}
	}

	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	request.BinlogPos = binlogPos
	request.Sources = sources

	resp := &pb.HandleErrorResponse{}
	err = common.SendRequest(
		ctx,
		"HandleError",
		request,
		&resp,
	)

	if err != nil {
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
