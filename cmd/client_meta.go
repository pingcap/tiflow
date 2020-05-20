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

package cmd

import (
	"github.com/spf13/cobra"
)

func newMetadataCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "meta",
		Short: "Manage metadata stored in PD",
	}
	command.AddCommand(
		newDeleteMetaCommand(),
	)
	return command
}

func newDeleteMetaCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "delete",
		Short: "Delete all meta data in etcd, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			err := cdcEtcdCli.ClearAllCDCInfo(ctx)
			if err == nil {
				cmd.Println("already truncate all meta in etcd!")
			}
			return err
		},
	}
	return command
}
