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

// newCmdDeleteServiceGcSafepoint creates the `cli unsafe delete-service-gc-safepoint` command.
func newCmdDeleteServiceGcSafepoint(f util.Factory, commonOptions *unsafeCommonOptions) *cobra.Command {
	command := &cobra.Command{
		Use:   "delete-service-gc-safepoint",
		Short: "Delete CDC service GC safepoint in PD, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := commonOptions.confirmMetaDelete(cmd); err != nil {
				return err
			}
			ctx := context.GetDefaultContext()
			pdClient, err := f.PdClient()
			if err != nil {
				return err
			}
			_, err = pdClient.UpdateServiceGCSafePoint(ctx, cdc.CDCServiceSafePointID, 0, 0)
			if err == nil {
				cmd.Println("CDC service GC safepoint truncated in PD!")
			}
			return errors.Trace(err)
		},
	}

	return command
}
