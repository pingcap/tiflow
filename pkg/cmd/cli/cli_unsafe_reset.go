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
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// newCmdReset creates the `cli unsafe reset` command.
func newCmdReset(f util.Factory, commonOptions *unsafeCommonOptions) *cobra.Command {
	command := &cobra.Command{
		Use:   "reset",
		Short: "Reset the status of the TiCDC cluster, delete all meta data in etcd, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := commonOptions.confirmMetaDelete(cmd); err != nil {
				return err
			}
			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			leases, err := etcdClient.GetCaptureLeases(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = etcdClient.ClearAllCDCInfo(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = etcdClient.RevokeAllLeases(ctx, leases)
			if err != nil {
				return errors.Trace(err)
			}

			pdClient, err := f.PdClient()
			if err != nil {
				return err
			}

			_, err = pdClient.UpdateServiceGCSafePoint(ctx, cdc.CDCServiceSafePointID, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}

			cmd.Println("reset and all metadata truncated in PD!")

			return nil
		},
	}

	return command
}
