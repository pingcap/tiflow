// Copyright 2024 PingCAP, Inc.
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
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// setSafePointOptions defines flags for the `cli safepoint query` command.
type setSafePointOptions struct {
	startTs         uint64
	ttl             int64
	serviceIDSuffix string
	clientV2        apiv2client.APIV2Interface
}

// newSetSafePointOptions creates new setSafePointOptions for the `cli safepoint query` command.
func newSetSafePointOptions() *setSafePointOptions {
	return &setSafePointOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *setSafePointOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.serviceIDSuffix, "service-id-suffix", "", "user-defined", "serviceIDSuffix")
	cmd.PersistentFlags().Uint64Var(&o.startTs, "start-ts", 0, "set cdc safepoint start-ts")
	cmd.PersistentFlags().Int64Var(&o.ttl, "ttl", 86400, "set gc-ttl")

}

// complete adapts from the command line args to the data and client required.
func (o *setSafePointOptions) complete(f factory.Factory) error {
	clientV2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.clientV2 = clientV2
	return nil
}

func (o *setSafePointOptions) setSafePoint(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()
	safepoint, err := o.clientV2.SafePoint().Set(ctx, &v2.SafePointConfig{
		StartTs:         o.startTs,
		ServiceIDSuffix: o.serviceIDSuffix,
		TTL:             o.ttl,
	})
	if err != nil {
		return err
	}
	return util.JSONPrint(cmd, safepoint)
}

// newCmdSetSafePoint creates the `cli safepoint set` command.
func newCmdSetSafePoint(f factory.Factory) *cobra.Command {
	o := newSetSafePointOptions()
	command := &cobra.Command{
		Use:   "set",
		Short: "Set safepoint",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.setSafePoint(cmd))
		},
	}
	o.addFlags(command)

	return command
}
