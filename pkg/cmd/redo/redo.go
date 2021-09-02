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

package redo

import (
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/logutil"
	"github.com/spf13/cobra"
)

// options defines flags for the `redo` command.
type options struct {
	logLevel string
}

// newOptions creates new options for the `server` command.
func newOptions() *options {
	return &options{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *options) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.logLevel, "log-level", "info", "log level (etc: debug|info|warn|error)")
}

// NewCmdRedo creates the `redo` command.
func NewCmdRedo() *cobra.Command {
	o := newOptions()

	cmds := &cobra.Command{
		Use:   "redo",
		Short: "Manage redo logs of TiCDC cluster",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Here we will initialize the logging configuration and set the current default context.
			util.InitCmd(cmd, &logutil.Config{Level: o.logLevel})
			util.LogHTTPProxies()
			return nil
		},
		Run: func(cmd *cobra.Command, args []string) {
		},
	}
	o.addFlags(cmds)

	// Add subcommands.
	cmds.AddCommand(newCmdApply())
	cmds.AddCommand(newCmdMeta())

	return cmds
}
