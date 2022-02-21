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
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
)

// unsafeDeleteServiceGcSafepointOptions defines flags
// for the `cli unsafe delete-service-gc-safepoint` command.
type unsafeDeleteServiceGcSafepointOptions struct {
	pdClient pd.Client
}

// newUnsafeDeleteServiceGcSafepointOptions creates new unsafeDeleteServiceGcSafepointOptions
// for the `cli unsafe delete-service-gc-safepoint` command.
func newUnsafeDeleteServiceGcSafepointOptions() *unsafeDeleteServiceGcSafepointOptions {
	return &unsafeDeleteServiceGcSafepointOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *unsafeDeleteServiceGcSafepointOptions) complete(f factory.Factory) error {
	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}

	o.pdClient = pdClient

	return nil
}

// run runs the `cli unsafe delete-service-gc-safepoint` command.
func (o *unsafeDeleteServiceGcSafepointOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	err := gc.RemoveServiceGCSafepoint(ctx, o.pdClient, gc.CDCServiceSafePointID)
	if err == nil {
		cmd.Println("CDC service GC safepoint truncated in PD!")
	}

	return errors.Trace(err)
}

// newCmdDeleteServiceGcSafepoint creates the `cli unsafe delete-service-gc-safepoint` command.
func newCmdDeleteServiceGcSafepoint(f factory.Factory, commonOptions *unsafeCommonOptions) *cobra.Command {
	o := newUnsafeDeleteServiceGcSafepointOptions()

	command := &cobra.Command{
		Use:   "delete-service-gc-safepoint",
		Short: "Delete CDC service GC safepoint in PD, confirm that you know what this command will do and use it at your own risk",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := commonOptions.confirmMetaDelete(cmd); err != nil {
				return err
			}

			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run(cmd)
		},
	}

	return command
}
