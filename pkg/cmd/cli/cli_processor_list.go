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
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// listProcessorOptions defines flags for the `cli processor list` command.
type listProcessorOptions struct {
	apiClient apiv1client.APIV1Interface
}

// newListProcessorOptions creates new listProcessorOptions for the `cli processor list` command.
func newListProcessorOptions() *listProcessorOptions {
	return &listProcessorOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *listProcessorOptions) complete(f factory.Factory) error {
	apiClient, err := f.APIV1Client()
	if err != nil {
		return err
	}
	o.apiClient = apiClient
	return nil
}

// run runs the `cli processor list` command.
func (o *listProcessorOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()
	processors, err := o.apiClient.Processors().List(ctx)
	if err != nil {
		return err
	}
	return util.JSONPrint(cmd, processors)
}

// newCmdListProcessor creates the `cli processor list` command.
func newCmdListProcessor(f factory.Factory) *cobra.Command {
	o := newListProcessorOptions()

	command := &cobra.Command{
		Use:   "list",
		Short: "List all processors in TiCDC cluster",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	return command
}
