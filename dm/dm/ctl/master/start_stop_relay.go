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

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"

	"github.com/spf13/cobra"
)

// NewStartRelayCmd creates a StartRelay command.
func NewStartRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start-relay <-s source-id>",
		Short: "Starts workers pulling relay log for a source",
		RunE:  startRelayFunc,
	}
	return cmd
}

// NewStopRelayCmd creates a StartRelay command.
func NewStopRelayCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-relay <-s source-id>",
		Short: "Stops workers pulling relay log for a source",
		RunE:  stopRelayFunc,
	}
	return cmd
}

func startRelayFunc(cmd *cobra.Command, _ []string) error {
	return startStopRelay(cmd, pb.RelayOpV2_StartRelayV2)
}

func stopRelayFunc(cmd *cobra.Command, _ []string) error {
	return startStopRelay(cmd, pb.RelayOpV2_StopRelayV2)
}

func startStopRelay(cmd *cobra.Command, op pb.RelayOpV2) error {
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	if len(cmd.Flags().Args()) == 0 && len(sources) == 0 {
		// all args empty
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	// TODO: support multiple sources and all sources
	if len(sources) != 1 {
		common.PrintLinesf("must specify one source (`-s` / `--source`)")
		return errors.New("please check output to see error")
	}

	workers := cmd.Flags().Args()
	if len(workers) > 0 {
		common.PrintLinesf("start-relay/stop-relay with worker name will be deprecated soon. You can try stopping relay first and use start-relay without worker name instead")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.OperateRelayResponse{}
	err = common.SendRequest(
		ctx,
		"OperateRelay",
		&pb.OperateRelayRequest{
			Op:     op,
			Source: sources[0],
			Worker: workers,
		},
		&resp,
	)

	if err != nil {
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
