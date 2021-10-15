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
	"encoding/json"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/dm/dm/config"
	"github.com/pingcap/dm/dm/ctl/common"
	"github.com/pingcap/dm/dm/pb"
	"github.com/pingcap/dm/pkg/ha"
	"github.com/pingcap/dm/pkg/utils"
)

var (
	taskDirname          = "tasks"
	sourceDirname        = "sources"
	relayWorkersFilename = "relay_workers.json"
	yamlSuffix           = ".yaml"
)

// NewConfigCmd creates a Config command.
func NewConfigCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config <command>",
		Short: "manage config operations",
	}
	cmd.AddCommand(
		newConfigTaskCmd(),
		newConfigSourceCmd(),
		newConfigMasterCmd(),
		newConfigWorkerCmd(),
		newExportCfgsCmd(),
		newImportCfgsCmd(),
	)
	cmd.PersistentFlags().StringP("path", "p", "", "specify the file path to export/import`")
	return cmd
}

func newConfigTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "task [task-name]",
		Short: "manage or show task configs",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 || len(args) > 1 {
				return cmd.Help()
			}
			name := args[0]
			output, err := cmd.Flags().GetString("path")
			if err != nil {
				return err
			}
			return sendGetConfigRequest(pb.CfgType_TaskType, name, output)
		},
	}
	cmd.AddCommand(
		newConfigTaskUpdateCmd(),
	)
	return cmd
}

// FIXME: implement this later.
func newConfigTaskUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "update <command>",
		Short:  "update config task",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.Errorf("this function will be supported later")
		},
	}
	return cmd
}

func newConfigSourceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "source [source-name]",
		Short: "manage or show source config",
		RunE:  configSourceList,
	}
	cmd.AddCommand(
		newConfigSourceUpdateCmd(),
	)
	return cmd
}

func configSourceList(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}
	name := args[0]
	output, err := cmd.Flags().GetString("path")
	if err != nil {
		return err
	}
	return sendGetConfigRequest(pb.CfgType_SourceType, name, output)
}

// FIXME: implement this later.
func newConfigSourceUpdateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:    "update <command>",
		Short:  "update config source",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return errors.Errorf("this function will be supported later")
		},
	}
	return cmd
}

func newConfigMasterCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "master [master-name]",
		Short: "manage or show master configs",
		RunE:  configMasterList,
	}
	return cmd
}

func configMasterList(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return cmd.Help()
	}
	name := args[0]
	output, err := cmd.Flags().GetString("path")
	if err != nil {
		return err
	}
	return sendGetConfigRequest(pb.CfgType_MasterType, name, output)
}

func newConfigWorkerCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "worker [worker-name]",
		Short: "manage or show worker configs",
		RunE:  configWorkerList,
	}
	return cmd
}

func configWorkerList(cmd *cobra.Command, args []string) error {
	if len(args) == 0 || len(args) > 1 {
		return cmd.Help()
	}
	name := args[0]
	output, err := cmd.Flags().GetString("path")
	if err != nil {
		return err
	}
	return sendGetConfigRequest(pb.CfgType_WorkerType, name, output)
}

// newExportCfgsCmd creates a exportCfg command.
func newExportCfgsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export the configurations of sources and tasks",
		RunE:  exportCfgsFunc,
	}
	cmd.Flags().StringP("dir", "d", "", "specify the configs directory, default is `./configs`")
	_ = cmd.Flags().MarkHidden("dir")
	return cmd
}

// newImportCfgsCmd creates a importCfg command.
func newImportCfgsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Import the configurations of sources and tasks",
		RunE:  importCfgsFunc,
	}
	cmd.Flags().StringP("dir", "d", "", "specify the configs directory, default is `./configs`")
	_ = cmd.Flags().MarkHidden("dir")
	return cmd
}

// exportCfgsFunc exports configs.
func exportCfgsFunc(cmd *cobra.Command, args []string) error {
	filePath, err := cmd.Flags().GetString("path")
	if err != nil {
		common.PrintLinesf("can not get path")
		return err
	} else if filePath == "" {
		filePath, err = cmd.Flags().GetString("dir")
		if err != nil {
			common.PrintLinesf("can not get directory")
			return err
		}
	}
	if filePath == "" {
		filePath = "configs"
	}

	// get all configs
	sourceCfgsMap, subTaskCfgsMap, relayWorkersSet, err := getAllCfgs(common.GlobalCtlClient.EtcdClient)
	if err != nil {
		return err
	}
	// create directory
	taskDir, sourceDir, err := createDirectory(filePath)
	if err != nil {
		return err
	}
	// write sourceCfg files
	if err = writeSourceCfgs(sourceDir, sourceCfgsMap); err != nil {
		return err
	}
	// write taskCfg files
	if err = writeTaskCfgs(taskDir, subTaskCfgsMap); err != nil {
		return err
	}
	// write relayWorkers
	if err = writeRelayWorkers(path.Join(filePath, relayWorkersFilename), relayWorkersSet); err != nil {
		return err
	}

	common.PrintLinesf("export configs to directory `%s` succeed", filePath)
	return nil
}

// importCfgsFunc imports configs.
func importCfgsFunc(cmd *cobra.Command, args []string) error {
	filePath, err := cmd.Flags().GetString("path")
	if err != nil {
		common.PrintLinesf("can not get path")
		return err
	} else if filePath == "" {
		filePath, err = cmd.Flags().GetString("dir")
		if err != nil {
			common.PrintLinesf("can not get directory")
			return err
		}
	}
	if filePath == "" {
		filePath = "configs"
	}

	sourceCfgs, taskCfgs, relayWorkers, err := collectCfgs(filePath)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := createSources(ctx, sourceCfgs); err != nil {
		return err
	}
	if err := createTasks(ctx, taskCfgs); err != nil {
		return err
	}
	if len(relayWorkers) > 0 {
		common.PrintLinesf("The original relay workers have been exported to `%s`.", path.Join(filePath, relayWorkersFilename))
		common.PrintLinesf("Currently DM doesn't support recover relay workers. You may need to execute `transfer-source` and `start-relay` command manually.")
	}

	common.PrintLinesf("import configs from directory `%s` succeed", filePath)
	return nil
}

func collectDirCfgs(dir string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	cfgs := make([]string, 0, len(files))
	for _, f := range files {
		cfg, err2 := common.GetFileContent(path.Join(dir, f.Name()))
		if err2 != nil {
			return nil, err2
		}
		cfgs = append(cfgs, string(cfg))
	}
	return cfgs, nil
}

// getSourceCfgs gets all source cfgs.
func getSourceCfgs(cli *clientv3.Client) (map[string]*config.SourceConfig, error) {
	sourceCfgsMap, _, err := ha.GetSourceCfg(cli, "", 0)
	if err != nil {
		return nil, err
	}
	// try to get all source cfgs before v2.0.2
	if len(sourceCfgsMap) == 0 {
		sourceCfgsMap, _, err = ha.GetAllSourceCfgBeforeV202(cli)
		if err != nil {
			return nil, err
		}
	}
	return sourceCfgsMap, nil
}

func getAllCfgs(cli *clientv3.Client) (map[string]*config.SourceConfig, map[string]map[string]config.SubTaskConfig, map[string]map[string]struct{}, error) {
	// get all source cfgs
	sourceCfgsMap, err := getSourceCfgs(cli)
	if err != nil {
		common.PrintLinesf("can not get source configs from etcd")
		return nil, nil, nil, err
	}
	// get all task cfgs
	subTaskCfgsMap, _, err := ha.GetAllSubTaskCfg(cli)
	if err != nil {
		common.PrintLinesf("can not get subtask configs from etcd")
		return nil, nil, nil, err
	}
	// get all relay configs.
	relayWorkers, _, err := ha.GetAllRelayConfig(cli)
	if err != nil {
		common.PrintLinesf("can not get relay workers from etcd")
		return nil, nil, nil, err
	}
	return sourceCfgsMap, subTaskCfgsMap, relayWorkers, nil
}

func createDirectory(dir string) (string, string, error) {
	taskDir := path.Join(dir, taskDirname)
	if err := os.MkdirAll(taskDir, 0o755); err != nil {
		common.PrintLinesf("can not create directory of task configs `%s`", taskDir)
		return "", "", err
	}
	sourceDir := path.Join(dir, sourceDirname)
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		common.PrintLinesf("can not create directory of source configs `%s`", sourceDir)
		return "", "", err
	}
	return taskDir, sourceDir, nil
}

func writeSourceCfgs(sourceDir string, sourceCfgsMap map[string]*config.SourceConfig) error {
	for source, sourceCfg := range sourceCfgsMap {
		sourceFile := path.Join(sourceDir, source)
		sourceFile += yamlSuffix
		fileContent, err := sourceCfg.YamlForDowngrade()
		if err != nil {
			common.PrintLinesf("fail to marshal source config of `%s`", source)
			return err
		}
		err = os.WriteFile(sourceFile, []byte(fileContent), 0o644)
		if err != nil {
			common.PrintLinesf("fail to write source config to file `%s`", sourceFile)
			return err
		}
	}
	return nil
}

func writeTaskCfgs(taskDir string, subTaskCfgsMap map[string]map[string]config.SubTaskConfig) error {
	subTaskCfgsListMap := make(map[string][]*config.SubTaskConfig, len(subTaskCfgsMap))
	// from source => task => subtask to task => subtask
	for _, subTaskCfgs := range subTaskCfgsMap {
		for task, subTaskCfg := range subTaskCfgs {
			clone := subTaskCfg
			subTaskCfgsListMap[task] = append(subTaskCfgsListMap[task], &clone)
		}
	}
	// from task => subtask to task => taskCfg
	for task, subTaskCfgs := range subTaskCfgsListMap {
		sort.Slice(subTaskCfgs, func(i, j int) bool {
			return subTaskCfgs[i].SourceID < subTaskCfgs[j].SourceID
		})
		taskCfg := config.FromSubTaskConfigs(subTaskCfgs...)

		taskFile := path.Join(taskDir, task)
		taskFile += yamlSuffix
		taskContent, err := taskCfg.YamlForDowngrade()
		if err != nil {
			common.PrintLinesf("fail to marshal source config of `%s`", task)
		}
		if err := os.WriteFile(taskFile, []byte(taskContent), 0o644); err != nil {
			common.PrintLinesf("can not write task config to file `%s`", taskFile)
			return err
		}
	}
	return nil
}

func writeRelayWorkers(relayWorkersFile string, relayWorkersSet map[string]map[string]struct{}) error {
	if len(relayWorkersSet) == 0 {
		return nil
	}

	// from source => workerSet to source => workerList
	relayWorkers := make(map[string][]string, len(relayWorkersSet))
	for source, workerSet := range relayWorkersSet {
		workers := make([]string, 0, len(workerSet))
		for worker := range workerSet {
			workers = append(workers, worker)
		}
		sort.Strings(workers)
		relayWorkers[source] = workers
	}

	content, err := json.Marshal(relayWorkers)
	if err != nil {
		common.PrintLinesf("fail to marshal relay workers")
		return err
	}

	err = os.WriteFile(relayWorkersFile, content, 0o644)
	if err != nil {
		common.PrintLinesf("can not write relay workers to file `%s`", relayWorkersFile)
		return err
	}
	return nil
}

func collectCfgs(dir string) (sourceCfgs []string, taskCfgs []string, relayWorkers map[string][]string, err error) {
	var (
		sourceDir        = path.Join(dir, sourceDirname)
		taskDir          = path.Join(dir, taskDirname)
		relayWorkersFile = path.Join(dir, relayWorkersFilename)
		content          []byte
	)
	if !utils.IsDirExists(dir) {
		return nil, nil, nil, errors.Errorf("config directory `%s` not exists", dir)
	}

	if utils.IsDirExists(sourceDir) {
		if sourceCfgs, err = collectDirCfgs(sourceDir); err != nil {
			common.PrintLinesf("fail to collect source config files from source configs directory `%s`", sourceDir)
			return
		}
	}
	if utils.IsDirExists(taskDir) {
		if taskCfgs, err = collectDirCfgs(taskDir); err != nil {
			common.PrintLinesf("fail to collect task config files from task configs directory `%s`", taskDir)
			return
		}
	}
	if utils.IsFileExists(relayWorkersFile) {
		content, err = common.GetFileContent(relayWorkersFile)
		if err != nil {
			common.PrintLinesf("fail to read relay workers config `%s`", relayWorkersFile)
			return
		}
		err = json.Unmarshal(content, &relayWorkers)
		if err != nil {
			common.PrintLinesf("fail to unmarshal relay workers config `%s`", relayWorkersFile)
			return
		}
	}
	// nolint:nakedret
	return
}

func createSources(ctx context.Context, sourceCfgs []string) error {
	if len(sourceCfgs) == 0 {
		return nil
	}
	common.PrintLinesf("start creating sources")

	sourceResp := &pb.OperateSourceResponse{}
	// Do not use batch for `operate-source start source1, source2` if we want to support idemponent config import.
	// Because `operate-source start` will revert all batch sources if any source error.
	// e.g. ErrSchedulerSourceCfgExist
	for _, sourceCfg := range sourceCfgs {
		err := common.SendRequest(
			ctx,
			"OperateSource",
			&pb.OperateSourceRequest{
				Config: []string{sourceCfg},
				Op:     pb.SourceOp_StartSource,
			},
			&sourceResp,
		)
		if err != nil {
			common.PrintLinesf("fail to create sources")
			return err
		}

		if !sourceResp.Result && !strings.Contains(sourceResp.Msg, "already exist") {
			common.PrettyPrintResponse(sourceResp)
			return errors.Errorf("fail to create sources")
		}
	}
	return nil
}

func createTasks(ctx context.Context, taskCfgs []string) error {
	if len(taskCfgs) == 0 {
		return nil
	}
	common.PrintLinesf("start creating tasks")

	taskResp := &pb.StartTaskResponse{}
	for _, taskCfg := range taskCfgs {
		err := common.SendRequest(
			ctx,
			"StartTask",
			&pb.StartTaskRequest{
				Task: taskCfg,
			},
			&taskResp,
		)
		if err != nil {
			common.PrintLinesf("fail to create tasks")
			return err
		}
		if !taskResp.Result && !strings.Contains(taskResp.Msg, "already exist") {
			common.PrettyPrintResponse(taskResp)
			return errors.Errorf("fail to create tasks")
		}
	}
	return nil
}
