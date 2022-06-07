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

package cli

import (
	"time"

	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

// unsafeResolveLockOptions defines flags for the `cli unsafe show-metadata` command.
type unsafeResolveLockOptions struct {
	apiClient *apiv2client.APIV2Client

	regionID uint64
	ts       uint64
}

// newUnsafeResolveLockOptions creates new unsafeResolveLockOptions
// for the `cli unsafe show-metadata` command.
func newUnsafeResolveLockOptions() *unsafeResolveLockOptions {
	return &unsafeResolveLockOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *unsafeResolveLockOptions) complete(f factory.Factory) error {
	ctx := context.GetDefaultContext()

	apiClient, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClient = apiClient
	if o.ts == 0 {
		tso, err := apiClient.Tso().Get(ctx)
		if err != nil {
			return err
		}
		now := oracle.GetTimeFromTS(oracle.ComposeTS(tso.Timestamp, tso.LogicTime))
		// Try not kill active transaction, we only resolves lock 1 minute ago.
		o.ts = oracle.GoTimeToTS(now.Add(-time.Minute))
	}
	return err
}

// run runs the `cli unsafe show-metadata` command.
func (o *unsafeResolveLockOptions) run() error {
	ctx := context.GetDefaultContext()
	return o.apiClient.Unsafe().ResolveLock(ctx, o.regionID, o.ts)
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *unsafeResolveLockOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.Flags().Uint64Var(&o.regionID, "region", 0, "Region ID")
	cmd.Flags().Uint64Var(&o.ts, "ts", 0,
		"resolve locks before the timestamp, default 1 minute ago from now")
	_ = cmd.MarkFlagRequired("region")
}

// newCmdResolveLock creates the `cli unsafe show-metadata` command.
func newCmdResolveLock(f factory.Factory) *cobra.Command {
	o := newUnsafeResolveLockOptions()

	command := &cobra.Command{
		Use:   "resolve-lock",
		Short: "resolve locks in regions",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run()
		},
	}

	o.addFlags(command)

	return command
}
