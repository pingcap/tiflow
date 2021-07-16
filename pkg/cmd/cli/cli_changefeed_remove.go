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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// removeChangefeedOptions defines flags for the `cli changefeed remove` command.
type removeChangefeedOptions struct {
	changefeedID   string
	optForceRemove bool
}

// newRemoveChangefeedOptions creates new options for the `cli changefeed remove` command.
func newRemoveChangefeedOptions() *removeChangefeedOptions {
	return &removeChangefeedOptions{}
}

// newCmdRemoveChangefeed creates the `cli changefeed remove` command.
func newCmdRemoveChangefeed(f util.Factory) *cobra.Command {
	o := newRemoveChangefeedOptions()

	command := &cobra.Command{
		Use:   "remove",
		Short: "Remove a replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			job := model.AdminJob{
				CfID: o.changefeedID,
				Type: model.AdminRemove,
				Opts: &model.AdminJobOption{
					ForceRemove: o.optForceRemove,
				},
			}

			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			return applyAdminChangefeed(ctx, etcdClient, job, f.GetCredential())
		},
	}

	command.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	command.PersistentFlags().BoolVarP(&o.optForceRemove, "force", "f", false, "remove all information of the changefeed")

	return command
}
