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
	"github.com/spf13/cobra"

	"github.com/pingcap/dm/dm/pb"
)

// NewStopTaskCmd creates a StopTask command.
func NewStopTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop-task [-s source ...] [task-name | task-file]",
		Short: "Stops a specified task or all (sub)tasks bound to a source",
		RunE:  stopTaskFunc,
	}
	addOperateSourceTaskFlags(cmd)
	return cmd
}

// stopTaskFunc does stop task request.
func stopTaskFunc(cmd *cobra.Command, _ []string) (err error) {
	return operateTaskFunc(pb.TaskOp_Stop, cmd)
}
