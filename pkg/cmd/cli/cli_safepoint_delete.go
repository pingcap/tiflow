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

// deleteSafePointOptions defines flags for the `cli safepoint query` command.
type deleteSafePointOptions struct {
	startTs         uint64
	serviceIDSuffix string
	clientV2        apiv2client.APIV2Interface
}

// newDeleteSafePointOptions creates new deleteSafePointOptions for the `cli safepoint query` command.
func newDeleteSafePointOptions() *deleteSafePointOptions {
	return &deleteSafePointOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *deleteSafePointOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.serviceIDSuffix, "service-id-suffix", "", "user-defined", "serviceIDSuffix")
	cmd.PersistentFlags().Uint64Var(&o.startTs, "start-ts", 0, "set cdc safepoint start-ts")

}

// complete adapts from the command line args to the data and client required.
func (o *deleteSafePointOptions) complete(f factory.Factory) error {
	clientV2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.clientV2 = clientV2
	return nil
}

func (o *deleteSafePointOptions) deleteSafePoint(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()
	safepoint, err := o.clientV2.SafePoint().Delete(ctx, &v2.SafePointConfig{
		StartTs:         o.startTs,
		ServiceIDSuffix: o.serviceIDSuffix,
	})
	if err != nil {
		return err
	}
	return util.JSONPrint(cmd, safepoint)
}

// newCmdDeleteSafePoint creates the `cli safepoint delete` command.
func newCmdDeleteSafePoint(f factory.Factory) *cobra.Command {
	o := newDeleteSafePointOptions()
	command := &cobra.Command{
		Use:   "delete",
		Short: "Delete safepoint",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.deleteSafePoint(cmd))
		},
	}
	o.addFlags(command)

	return command
}
