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
	"github.com/pingcap/tiflow/dm/pb"
	"github.com/spf13/cobra"
)

// NewPauseTaskCmd creates a PauseTask command.
func NewPauseTaskCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   `pause-task [-s source ...] [task-name | task-file]`,
		Short: "Pauses a specified running task or all (sub)tasks bound to a source",
		RunE:  pauseTaskFunc,
	}
	addOperateSourceTaskFlags(cmd)
	return cmd
}

// pauseTaskFunc does pause task request.
func pauseTaskFunc(cmd *cobra.Command, _ []string) (err error) {
	return operateTaskFunc(pb.TaskOp_Pause, cmd)
}
