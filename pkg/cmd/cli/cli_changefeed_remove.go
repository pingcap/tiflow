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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/spf13/cobra"
)

// removeChangefeedOptions defines flags for the `cli changefeed remove` command.
type removeChangefeedOptions struct {
	etcdClient *etcd.CDCEtcdClient

	credential *security.Credential

	changefeedID   string
	optForceRemove bool
}

// newRemoveChangefeedOptions creates new options for the `cli changefeed remove` command.
func newRemoveChangefeedOptions() *removeChangefeedOptions {
	return &removeChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *removeChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().BoolVarP(&o.optForceRemove, "force", "f", false, "remove all information of the changefeed")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *removeChangefeedOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	o.credential = f.GetCredential()

	return nil
}

// run the `cli changefeed remove` command.
func (o *removeChangefeedOptions) run() error {
	job := model.AdminJob{
		CfID: model.DefaultChangeFeedID(o.changefeedID),
		Type: model.AdminRemove,
		Opts: &model.AdminJobOption{
			ForceRemove: o.optForceRemove,
		},
	}

	ctx := context.GetDefaultContext()

	return sendOwnerAdminChangeQuery(ctx, o.etcdClient, job, o.credential)
}

// newCmdRemoveChangefeed creates the `cli changefeed remove` command.
func newCmdRemoveChangefeed(f factory.Factory) *cobra.Command {
	o := newRemoveChangefeedOptions()

	command := &cobra.Command{
		Use:   "remove",
		Short: "Remove a replication task (changefeed)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
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
