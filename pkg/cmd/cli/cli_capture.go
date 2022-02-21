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

package cli

import (
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// captureOptions defines flags for the `cli capture` command.
type captureOptions struct {
	disableVersionCheck bool
}

// newCaptureOptions creates new captureOptions for the `cli capture` command.
func newCaptureOptions() *captureOptions {
	return &captureOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *captureOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.PersistentFlags().BoolVar(&o.disableVersionCheck, "disable-version-check", false, "Disable version check")
	_ = cmd.PersistentFlags().MarkHidden("disable-version-check")
}

// run checks the TiCDC cluster version.
func (o *captureOptions) run(f factory.Factory) error {
	if o.disableVersionCheck {
		return nil
	}
	ctx := cmdcontext.GetDefaultContext()
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	_, err = util.VerifyAndGetTiCDCClusterVersion(ctx, etcdClient)
	if err != nil {
		return err
	}
	return nil
}

// newCmdCapture creates the `cli capture` command.
func newCmdCapture(f factory.Factory) *cobra.Command {
	o := newCaptureOptions()

	cmds := &cobra.Command{
		Use:   "capture",
		Short: "Manage capture (capture is a CDC server instance)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return o.run(f)
		},
	}
	cmds.AddCommand(
		newCmdListCapture(f),
		// TODO: add resign owner command
	)

	o.addFlags(cmds)

	return cmds
}
