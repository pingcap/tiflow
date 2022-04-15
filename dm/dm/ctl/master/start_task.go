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

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
)

// NewStartTaskCmd creates a StartTask command.
func NewStartTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start-task [-s source ...] [--remove-meta] <config-file>",
		Short: "Starts a task as defined in the configuration file",
		RunE:  startTaskFunc,
	}
	cmd.Flags().BoolP("remove-meta", "", false, "whether to remove task's meta data")
	cmd.Flags().String("start-time", "", "specify the start time of binlog replication, e.g. '2021-10-21 00:01:00' or 2021-10-21T00:01:00")
	return cmd
}

// startTaskFunc does start task request.
func startTaskFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	// If task's target db is configured with tls certificate related content
	// the contents of the certificate need to be read and transferred to the dm-master
	task := config.NewTaskConfig()
	yamlErr := task.RawDecode(string(content))
	if yamlErr != nil {
		return yamlErr
	}
	if task.TargetDB != nil && task.TargetDB.Security != nil {
		loadErr := task.TargetDB.Security.LoadTLSContent()
		if loadErr != nil {
			log.L().Warn("load tls content failed", zap.Error(terror.ErrCtlLoadTLSCfg.Generate(loadErr)))
		}
		content = []byte(task.String())
	}
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}

	removeMeta, err := cmd.Flags().GetBool("remove-meta")
	if err != nil {
		common.PrintLinesf("error in parse `--remove-meta`")
		return err
	}
	startTime, err := cmd.Flags().GetString("start-time")
	if err != nil {
		common.PrintLinesf("error in parse `--start-time`")
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start task
	resp := &pb.StartTaskResponse{}
	err = common.SendRequest(
		ctx,
		"StartTask",
		&pb.StartTaskRequest{
			Task:       string(content),
			Sources:    sources,
			RemoveMeta: removeMeta,
			StartTime:  startTime,
		},
		&resp,
	)

	if err != nil {
		return err
	}

	if !common.PrettyPrintResponseWithCheckTask(resp, checker.CheckTaskMsgHeader) {
		common.PrettyPrintResponse(resp)
	}
	return nil
}
