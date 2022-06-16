// Copyright 2022 PingCAP, Inc.
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
	"net/http"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/openapi"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewTaskCreateCmd creates a Task Create command.
func NewTaskCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <config-file>",
		Short: "Create a task by task configuration file",
		RunE:  taskCreateFunc,
	}
	return cmd
}

func taskCreateFunc(cmd *cobra.Command, _ []string) error {
	if len(cmd.Flags().Args()) != 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}
	res, err := createTask(cmd.Flags().Arg(0))
	if err != nil {
		return errors.Trace(err)
	}
	common.PrettyPrintOpenapiResp(true, res)
	return nil
}

func createTask(filePath string) (*openapi.OperateTaskResponse, error) {
	task, err := convertToOpenapiTaskFromFile(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// create task
	params := []interface{}{openapi.CreateTaskRequest{Task: *task}}
	createResp := &openapi.OperateTaskResponse{}
	sendError := common.SendOpenapiRequest(ctx, "DMAPICreateTask", params, http.StatusCreated, createResp)
	if sendError != nil {
		return nil, errors.Trace(sendError)
	}
	return createResp, nil
}

func convertToOpenapiTaskFromFile(filePath string) (*openapi.Task, error) {
	var (
		err         error
		sendError   *common.SendToOpenapiError
		fileContent []byte
		taskStr     string
		params      []interface{}
	)
	fileContent, err = common.GetFileContent(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}

	task := config.NewTaskConfig()
	err = task.RawDecode(string(fileContent))
	if err != nil {
		return nil, err
	}
	if task.TargetDB != nil && task.TargetDB.Security != nil {
		loadErr := task.TargetDB.Security.LoadTLSContent()
		if loadErr != nil {
			log.L().Warn("load tls content failed", zap.Error(terror.ErrCtlLoadTLSCfg.Generate(loadErr)))
		}
		fileContent = []byte(task.String())
	}

	lines := bytes.Split(fileContent, []byte("\n"))
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

	if isShardingSet && !task.IsSharding && task.ShardMode != "" {
		common.PrintLinesf("The behaviour of `is-sharding` and `shard-mode` is conflicting. `is-sharding` is deprecated, please use `shard-mode` only.")
		return nil, errors.New("please check output to see error")
	}
	if shardModeSet && task.ShardMode == "" && task.IsSharding {
		common.PrintLinesf("The behaviour of `is-sharding` and `shard-mode` is conflicting. `is-sharding` is deprecated, please use `shard-mode` only.")
		return nil, errors.New("please check output to see error")
	}

	taskStr = string(fileContent)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// convert to openapi task
	params = []interface{}{openapi.ConverterTaskRequest{TaskConfigFile: &taskStr}}
	convertRes := &openapi.ConverterTaskResponse{}
	sendError = common.SendOpenapiRequest(ctx, "DMAPIConvertTask", params, http.StatusOK, convertRes)
	if sendError != nil {
		return nil, errors.Trace(sendError)
	}

	return &convertRes.Task, nil
}
