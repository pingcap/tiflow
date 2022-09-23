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
	"time"

	"github.com/pingcap/tiflow/cdc/api/owner"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

const timeFormat = "2006-01-02 15:04:05.000"

// changefeedCommonInfo holds some common used information of a changefeed.
type changefeedCommonInfo struct {
	ID        string                `json:"id"`
	Namespace string                `json:"namespace"`
	Summary   *owner.ChangefeedResp `json:"summary"`
}

// listChangefeedOptions defines flags for the `cli changefeed list` command.
type listChangefeedOptions struct {
	apiClient apiv1client.APIV1Interface

	listAll bool
}

// newListChangefeedOptions creates new options for the `cli changefeed list` command.
func newListChangefeedOptions() *listChangefeedOptions {
	return &listChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *listChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVarP(&o.listAll, "all", "a", false, "List all replication tasks(including removed and finished)")
}

// complete adapts from the command line args to the data and client required.
func (o *listChangefeedOptions) complete(f factory.Factory) error {
	apiClient, err := f.APIV1Client()
	if err != nil {
		return err
	}
	o.apiClient = apiClient
	return nil
}

// run the `cli changefeed list` command.
func (o *listChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	raw, err := o.apiClient.Changefeeds().List(ctx, "all")
	if err != nil {
		return err
	}
	cfs := make([]*changefeedCommonInfo, 0, len(*raw))

	for _, cf := range *raw {
		if !o.listAll {
			if cf.FeedState == model.StateFinished ||
				cf.FeedState == model.StateRemoved {
				continue
			}
		}
		cfci := &changefeedCommonInfo{
			ID:        cf.ID,
			Namespace: cf.Namespace,
			Summary: &owner.ChangefeedResp{
				FeedState:    string(cf.FeedState),
				TSO:          cf.CheckpointTSO,
				Checkpoint:   time.Time(cf.CheckpointTime).Format(timeFormat),
				RunningError: cf.RunningError,
			},
		}
		cfs = append(cfs, cfci)
	}

	return util.JSONPrint(cmd, cfs)
}

// newCmdListChangefeed creates the `cli changefeed list` command.
func newCmdListChangefeed(f factory.Factory) *cobra.Command {
	o := newListChangefeedOptions()

	command := &cobra.Command{
		Use:   "list",
		Short: "List all replication tasks (changefeeds) in TiCDC cluster",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
