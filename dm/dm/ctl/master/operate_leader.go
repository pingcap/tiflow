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

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

// NewOperateLeaderCmd creates a OperateLeader command.
func NewOperateLeaderCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operate-leader <operate-type>",
		Short: "`evict`/`cancel-evict` the leader",
		RunE:  operateLeaderFunc,
	}
	return cmd
}

func convertOpType(op string) pb.LeaderOp {
	switch op {
	case "evict":
		return pb.LeaderOp_EvictLeaderOp
	case "cancel-evict":
		return pb.LeaderOp_CancelEvictLeaderOp
	default:
		return pb.LeaderOp_InvalidLeaderOp
	}
}

// operateLeaderFunc does operate leader request.
func operateLeaderFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	opType := cmd.Flags().Arg(0)

	op := convertOpType(opType)
	if op == pb.LeaderOp_InvalidLeaderOp {
		common.PrintLinesf("invalid operate '%s' on leader", opType)
		return errors.New("please check output to see error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// operate leader
	resp := &pb.OperateLeaderResponse{}
	err := common.SendRequest(
		ctx,
		"OperateLeader",
		&pb.OperateLeaderRequest{
			Op: op,
		},
		&resp,
	)
	if err != nil {
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
