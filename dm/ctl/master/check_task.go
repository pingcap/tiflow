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
	"bytes"
	"context"
	"errors"
	"os"

	"github.com/pingcap/tiflow/dm/checker"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/spf13/cobra"
)

// NewCheckTaskCmd creates a CheckTask command.
func NewCheckTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check-task <config-file> [--error count] [--warn count]",
		Short: "Checks the configuration file of the task",
		RunE:  checkTaskFunc,
	}
	cmd.Flags().Int64P("error", "e", common.DefaultErrorCnt, "max count of errors to display")
	cmd.Flags().Int64P("warn", "w", common.DefaultWarnCnt, "max count of warns to display")
	cmd.Flags().String("start-time", "", "specify the start time of binlog replication, e.g. '2021-10-21 00:01:00' or 2021-10-21T00:01:00")
	return cmd
}

// checkTaskFunc does check task request.
func checkTaskFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	content, err := common.GetFileContent(cmd.Flags().Arg(0))
	if err != nil {
		return err
	}

	errCnt, err := cmd.Flags().GetInt64("error")
	if err != nil {
		return err
	}
	warnCnt, err := cmd.Flags().GetInt64("warn")
	if err != nil {
		return err
	}
	startTime, err := cmd.Flags().GetString("start-time")
	if err != nil {
		return err
	}

	lines := bytes.Split(content, []byte("\n"))
	// we check if `is-sharding` is explicitly set, to distinguish between `false` from default value
	isShardingSet := false
	for i := range lines {
		if bytes.HasPrefix(lines[i], []byte("is-sharding")) {
			isShardingSet = true
			break
		}
	}
	// we check if `shard-mode` is explicitly set, to distinguish between "" from default value
	shardModeSet := false
	for i := range lines {
		if bytes.HasPrefix(lines[i], []byte("shard-mode")) {
			shardModeSet = true
			break
		}
	}

	task := config.NewTaskConfig()
	yamlErr := task.RawDecode(string(content))
	// if can't parse yaml, we ignore this check
	if yamlErr == nil {
		if isShardingSet && !task.IsSharding && task.ShardMode != "" {
			common.PrintLinesf("The behaviour of `is-sharding` and `shard-mode` is conflicting. `is-sharding` is deprecated, please use `shard-mode` only.")
			return errors.New("please check output to see error")
		}
		if shardModeSet && task.ShardMode == "" && task.IsSharding {
			common.PrintLinesf("The behaviour of `is-sharding` and `shard-mode` is conflicting. `is-sharding` is deprecated, please use `shard-mode` only.")
			return errors.New("please check output to see error")
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.CheckTaskResponse{}
	err = common.SendRequest(
		ctx,
		"CheckTask",
		&pb.CheckTaskRequest{
			Task:      string(content),
			ErrCnt:    errCnt,
			WarnCnt:   warnCnt,
			StartTime: startTime,
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
