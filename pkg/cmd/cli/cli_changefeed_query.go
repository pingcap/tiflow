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
	"context"

	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/spf13/cobra"
)

// queryChangefeedOptions defines flags for the `cli changefeed query` command.
type queryChangefeedOptions struct {
	apiClient *apiv1client.APIV1Client

	credential   *security.Credential
	changefeedID string
	simplified   bool
}

// newQueryChangefeedOptions creates new options for the `cli changefeed query` command.
func newQueryChangefeedOptions() *queryChangefeedOptions {
	return &queryChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *queryChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVarP(&o.simplified, "simple", "s", false, "Output simplified replication status")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *queryChangefeedOptions) complete(f factory.Factory) error {
	o.credential = f.GetCredential()
	client, err := f.APIV1Client()
	if err != nil {
		return err
	}
	o.apiClient = client
	return nil
}

// run the `cli changefeed query` command.
func (o *queryChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.Background()
	if o.simplified {
		infos, err := o.apiClient.Changefeeds().List(ctx, "all")
		if err != nil {
			return nil
		}
		for _, info := range *infos {
			if info.ID == o.changefeedID {
				return util.JSONPrint(cmd, info)
			}
		}
		return cerror.ErrChangeFeedNotExists.GenWithStackByArgs(o.changefeedID)
	}
	detail, err := o.apiClient.Changefeeds().Get(ctx, o.changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}
	return util.JSONPrint(cmd, detail)
}

// newCmdQueryChangefeed creates the `cli changefeed query` command.
func newCmdQueryChangefeed(f factory.Factory) *cobra.Command {
	o := newQueryChangefeedOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replication task (changefeed)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run(cmd)
		},
	}

	o.addFlags(command)

	return command
}
