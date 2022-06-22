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
	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/spf13/cobra"
)

// NewTaskCheckCmd creates a task check command.
func NewTaskCheckCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check <config-file> [--error count] [--warn count] [--start-time]",
		Short: "Check the configuration file of the task",
		// TODO check task by openapi
		RunE: checkTaskFunc,
	}
	cmd.Flags().Int64P("error", "e", common.DefaultErrorCnt, "max count of errors to display")
	cmd.Flags().Int64P("warn", "w", common.DefaultWarnCnt, "max count of warns to display")
	cmd.Flags().String("start-time", "", "specify the start time of binlog replication, e.g. '2021-10-21 00:01:00' or 2021-10-21T00:01:00")
	return cmd
}
