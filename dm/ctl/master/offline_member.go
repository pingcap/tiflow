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
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/spf13/cobra"
)

// NewOfflineMemberCmd creates an OfflineWorker command.
func NewOfflineMemberCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "offline-member <--master/--worker> <--name master-name/worker-name>",
		Short: "Offlines member which has been closed",
		RunE:  offlineMemberFunc,
	}
	cmd.Flags().BoolP("master", "m", false, "to offline a master")
	cmd.Flags().BoolP("worker", "w", false, "to offline a worker")
	cmd.Flags().StringP("name", "n", "", "specify member name for choosing type")
	return cmd
}

func convertOfflineMemberType(cmd *cobra.Command) (string, error) {
	master, err := cmd.Flags().GetBool("master")
	if err != nil {
		return "", err
	}
	worker, err := cmd.Flags().GetBool("worker")
	if err != nil {
		return "", err
	}
	if (master && worker) || (!master && !worker) {
		return "", errors.New("should specify either --master or --worker")
	}
	if master {
		return common.Master, nil
	}
	return common.Worker, nil
}

// offlineMemberFunc does offline member request.
func offlineMemberFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) > 0 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	offlineType, err := convertOfflineMemberType(cmd)
	if err != nil {
		common.PrintLinesf("get offline type failed")
		return err
	}
	name, err := cmd.Flags().GetString("name")
	if err != nil {
		common.PrintLinesf("get offline name failed")
		return err
	} else if name == "" {
		common.PrintLinesf("a member name must be specified")
		return errors.New("please check output to see error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.OfflineMemberResponse{}
	err = common.SendRequest(
		ctx,
		"OfflineMember",
		&pb.OfflineMemberRequest{
			Type: offlineType,
			Name: name,
		},
		&resp,
	)

	if err != nil {
		return err
	}
	if !resp.Result {
		common.PrintLinesf("offline member failed:\n%v", resp.Msg)
		return errors.New("please check output to see error")
	}
	common.PrettyPrintResponse(resp)
	return nil
}
