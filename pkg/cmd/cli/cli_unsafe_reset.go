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
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
)

// unsafeResetOptions defines flags for the `cli unsafe reset` command.
type unsafeResetOptions struct {
	etcdClient *etcd.CDCEtcdClient
	pdClient   pd.Client
}

// newUnsafeResetOptions creates new unsafeResetOptions for the `cli unsafe reset` command.
func newUnsafeResetOptions() *unsafeResetOptions {
	return &unsafeResetOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *unsafeResetOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}

	o.pdClient = pdClient

	return nil
}

// run runs the `cli unsafe reset` command.
func (o *unsafeResetOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	leases, err := o.etcdClient.GetCaptureLeases(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.etcdClient.ClearAllCDCInfo(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	err = o.etcdClient.RevokeAllLeases(ctx, leases)
	if err != nil {
		return errors.Trace(err)
	}

	err = gc.RemoveServiceGCSafepoint(ctx, o.pdClient, gc.CDCServiceSafePointID)
	if err != nil {
		return errors.Trace(err)
	}

	cmd.Println("reset and all metadata truncated in PD!")

	return nil
}

// newCmdReset creates the `cli unsafe reset` command.
func newCmdReset(f factory.Factory, commonOptions *unsafeCommonOptions) *cobra.Command {
	o := newUnsafeResetOptions()

	command := &cobra.Command{
		Use:   "reset",
		Short: "Reset the status of the TiCDC cluster, delete all meta data in etcd, confirm that you know what this command will do and use it at your own risk",
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
