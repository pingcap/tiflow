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
	"strings"

	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
)

// removeChangefeedOptions defines flags for the `cli changefeed remove` command.
type removeChangefeedOptions struct {
	apiClient    apiv2client.APIV2Interface
	changefeedID string
	namespace    string
}

// newRemoveChangefeedOptions creates new options for the `cli changefeed remove` command.
func newRemoveChangefeedOptions() *removeChangefeedOptions {
	return &removeChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *removeChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.namespace, "namespace", "n", "default", "Replication task (changefeed) Namespace")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *removeChangefeedOptions) complete(f factory.Factory) error {
	client, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClient = client
	return nil
}

// run the `cli changefeed remove` command.
func (o *removeChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	changefeedDetail, err := o.apiClient.Changefeeds().Get(ctx, o.namespace, o.changefeedID)
	if err != nil {
		if strings.Contains(err.Error(), "ErrChangeFeedNotExists") {
			cmd.Printf("Changefeed not found.\nID: %s\n", o.changefeedID)
			return nil
		}

		cmd.Printf("Changefeed remove failed.\nID: %s\nError: %s\n", o.changefeedID,
			err.Error())
		return err
	}
	checkpointTs := changefeedDetail.CheckpointTs
	sinkURI := changefeedDetail.SinkURI

	err = o.apiClient.Changefeeds().Delete(ctx, o.namespace, o.changefeedID)
	if err != nil {
		cmd.Printf("Changefeed remove failed.\nID: %s\nError: %s\n", o.changefeedID,
			err.Error())
		return err
	}

	_, err = o.apiClient.Changefeeds().Get(ctx, o.namespace, o.changefeedID)
	// Should never happen here. This checking is for defending.
	// The reason is that changefeed query to owner is invoked in the subsequent owner
	// Tick and in that Tick, the in-memory data structure and the metadata stored in
	// etcd is already deleted.
	if err == nil {
		err = cerror.ErrChangeFeedDeletionUnfinished.GenWithStackByArgs(o.changefeedID)
	}

	if strings.Contains(err.Error(), "ErrChangeFeedNotExists") {
		err = nil
	}

	if err == nil {
		cmd.Printf("Changefeed remove successfully.\nID: %s\nCheckpointTs: %d\nSinkURI: %s\n",
			o.changefeedID, checkpointTs, sinkURI)
	} else {
		cmd.Printf("Changefeed remove failed.\nID: %s\nError: %s\n", o.changefeedID,
			err.Error())
	}

	return err
}

// newCmdRemoveChangefeed creates the `cli changefeed remove` command.
func newCmdRemoveChangefeed(f factory.Factory) *cobra.Command {
	o := newRemoveChangefeedOptions()

	command := &cobra.Command{
		Use:   "remove",
		Short: "Remove a replication task (changefeed)",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
