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

package processor

import (
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// newCmdListProcessor creates the `cli processor list` command.
func newCmdListProcessor(f util.Factory) *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all processors in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}
			info, err := etcdClient.GetProcessors(ctx)
			if err != nil {
				return err
			}
			return util.JsonPrint(cmd, info)
		},
	}

	return command
}
