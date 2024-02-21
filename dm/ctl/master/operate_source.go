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
	"fmt"
	"os"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// NewOperateSourceCmd creates a OperateSource command.
func NewOperateSourceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operate-source <operate-type> [config-file ...] [-w worker] [--print-sample-config]",
		Short: "`create`/`stop`/`show` upstream MySQL/MariaDB source",
		RunE:  operateSourceFunc,
	}
	cmd.Flags().BoolP("print-sample-config", "p", false, "print sample config file of source")
	cmd.Flags().StringP("worker", "w", "", "specify bound worker for created source")
	return cmd
}

func convertCmdType(t string) pb.SourceOp {
	switch t {
	case "create":
		return pb.SourceOp_StartSource
	case "update":
		return pb.SourceOp_UpdateSource
	case "stop":
		return pb.SourceOp_StopSource
	case "show":
		return pb.SourceOp_ShowSource
	default:
		return pb.SourceOp_InvalidSourceOp
	}
}

// operateMysqlFunc does migrate relay request.
func operateSourceFunc(cmd *cobra.Command, _ []string) error {
	printSampleConfig, err := cmd.Flags().GetBool("print-sample-config")
	if err != nil {
		common.PrintLinesf("error in parse `--print-sample-config`")
		return err
	}

	if printSampleConfig {
		fmt.Println(config.SampleSourceConfig)
		return nil
	}

	if len(cmd.Flags().Args()) < 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("please check output to see error")
	}

	cmdType := cmd.Flags().Arg(0)
	op := convertCmdType(cmdType)
	if op == pb.SourceOp_InvalidSourceOp {
		common.PrintLinesf("invalid operate '%s' on worker", cmdType)
		return errors.New("please check output to see error")
	}
	if op != pb.SourceOp_ShowSource && len(cmd.Flags().Args()) == 1 {
		common.PrintLinesf("operate-source create/stop should specify config-file(s)")
		return errors.New("please check output to see error")
	}

	var specifyWorker string
	if op == pb.SourceOp_StartSource {
		specifyWorker, err = cmd.Flags().GetString("worker")
		if err != nil {
			common.PrintLinesf("error in parse `--worker`")
			return err
		}
		if specifyWorker != "" {
			if len(cmd.Flags().Args()) > 2 {
				common.PrintLinesf("operate-source create can't create multiple sources when specify worker")
			}
		}
	}

	contents := make([]string, 0, len(cmd.Flags().Args())-1)
	sourceID := make([]string, 0, len(cmd.Flags().Args())-1)
	sources, err := common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}
	sourceID = append(sourceID, sources...)

	for i := 1; i < len(cmd.Flags().Args()); i++ {
		arg := cmd.Flags().Arg(i)
		var content []byte
		content, err = common.GetFileContent(arg)
		if err != nil {
			if op == pb.SourceOp_StopSource {
				sourceID = append(sourceID, arg)
				continue
			}
			return err
		}
		// If source is configured with tls certificate related content
		// the contents of the certificate need to be read and transferred to the dm-master
		cfg, yamlErr := config.SourceCfgFromYaml(string(content))
		if yamlErr != nil {
			return yamlErr
		}
		if cfg.From.Security != nil {
			loadErr := cfg.From.Security.LoadTLSContent()
			if loadErr != nil {
				log.L().Warn("load tls content failed", zap.Error(terror.ErrCtlLoadTLSCfg.Generate(loadErr)))
			}
			yamlStr, yamlErr := cfg.Yaml()
			if yamlErr != nil {
				return yamlErr
			}
			content = []byte(yamlStr)
		}
		if cfg.RelayDir != "" {
			common.PrintLinesf("`relay-dir` in source config will be deprecated soon, please use `relay-dir` in worker config instead")
		}
		contents = append(contents, string(content))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.OperateSourceResponse{}
	err = common.SendRequest(
		ctx,
		"OperateSource",
		&pb.OperateSourceRequest{
			Config:     contents,
			Op:         op,
			SourceID:   sourceID,
			WorkerName: specifyWorker,
		},
		&resp,
	)

	if err != nil {
		return err
	}

	common.PrettyPrintResponse(resp)
	return nil
}
