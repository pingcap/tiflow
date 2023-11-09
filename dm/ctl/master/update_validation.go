// Copyright 2023 PingCAP, Inc.
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

	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/binlog"
	"github.com/spf13/cobra"
)

const (
	UpdateValidationOp = "update"
)

type validationUpdateArgs struct {
	sources  []string
	taskName string

	cutoverBinlogPos  string
	cutoverBinlogGTID string
}

func NewUpdateValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update [task-name]",
		Short: "update validation config of the completeness of the data",
		RunE:  updateValidation,
	}
	cmd.Flags().String("cutover-binlog-pos", "", "specify the cutover binlog name for validation, should include binlog name and pos in brackets, e.g. '(mysql-bin.000001, 5989)'")
	cmd.Flags().String("cutover-binlog-gtid", "", "specify the cutover binlog gtid for validation, only valid when source config's gtid is enabled, e.g. '1642618e-cf65-11ec-9e3d-0242ac110002:1-30'")
	return cmd
}

func updateValidation(cmd *cobra.Command, _ []string) error {
	args, msg, ok := parseValidationUpdateArgs(cmd)
	if !ok {
		return printUsageAndFailWithMessage(cmd, msg)
	}
	req := &pb.UpdateValidationRequest{
		TaskName:   args.taskName,
		Sources:    args.sources,
		BinlogPos:  args.cutoverBinlogPos,
		BinlogGTID: args.cutoverBinlogGTID,
	}

	resp := &pb.UpdateValidationResponse{}
	err := common.SendRequest(context.Background(), "UpdateValidation", req, &resp)
	if err != nil {
		return err
	}
	common.PrettyPrintResponse(resp)
	return nil
}

func parseValidationUpdateArgs(cmd *cobra.Command) (validationUpdateArgs, string, bool) {
	var err error
	args := validationUpdateArgs{}
	if args.sources, err = common.GetSourceArgs(cmd); err != nil {
		return args, err.Error(), false
	}
	if args.cutoverBinlogPos, err = cmd.Flags().GetString("cutover-binlog-pos"); err != nil {
		return args, err.Error(), false
	}
	if len(args.cutoverBinlogPos) != 0 {
		_, err = binlog.PositionFromPosStr(args.cutoverBinlogPos)
		if err != nil {
			return args, err.Error(), false
		}
	}

	if args.cutoverBinlogGTID, err = cmd.Flags().GetString("cutover-binlog-gtid"); err != nil {
		return args, err.Error(), false
	}
	if len(args.cutoverBinlogGTID) != 0 && len(args.cutoverBinlogPos) != 0 {
		return args, "you must specify either one of '--cutover-binlog-pos' or '--cutover-binlog-pos'", false
	}

	if len(cmd.Flags().Args()) == 0 {
		return args, "`task-name` should be set", false
	} else if len(cmd.Flags().Args()) > 1 {
		return args, "should specify only one `task-name`", false
	}
	args.taskName = cmd.Flags().Arg(0)
	if len(args.taskName) == 0 {
		return args, "`task-name` should be set", false
	}
	return args, "", true
}
