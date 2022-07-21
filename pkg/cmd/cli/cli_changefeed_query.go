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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"

	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	cutil "github.com/pingcap/tiflow/pkg/util"
	"github.com/spf13/cobra"
)

// cfMeta holds changefeed info and changefeed status.
type cfMeta struct {
	Info   *model.ChangeFeedInfo   `json:"info"`
	Status *model.ChangeFeedStatus `json:"status"`
}

// queryChangefeedOptions defines flags for the `cli changefeed query` command.
type queryChangefeedOptions struct {
	apiClient    apiv1client.APIV1Interface
	apiClientV2  apiv2client.APIV2Interface
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
	clientV1, err := f.APIV1Client()
	if err != nil {
		return err
	}
	o.apiClient = clientV1
	clientV2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClientV2 = clientV2
	return nil
}

// run the `cli changefeed query` command.
func (o *queryChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.Background()
	if o.simplified {
		infos, err := o.apiClient.Changefeeds().List(ctx, "all")
		if err != nil {
			return errors.Trace(err)
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

	info, err := o.apiClientV2.Changefeeds().GetInfo(ctx, o.changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}
	var runningError *model.RunningError
	if info.Error != nil {
		runningError = &model.RunningError{
			Addr:    info.Error.Addr,
			Code:    info.Error.Code,
			Message: info.Error.Message,
		}
	}

	sinkURI, err := cutil.MaskSinkURI(info.SinkURI)
	if err != nil {
		return err
	}

	meta := &cfMeta{
		Info: &model.ChangeFeedInfo{
			UpstreamID:        info.UpstreamID,
			Namespace:         info.Namespace,
			ID:                info.ID,
			SinkURI:           sinkURI,
			CreateTime:        info.CreateTime,
			StartTs:           info.StartTs,
			TargetTs:          info.TargetTs,
			AdminJobType:      info.AdminJobType,
			Engine:            info.Engine,
			Config:            info.Config.ToInternalReplicaConfig(),
			State:             info.State,
			Error:             runningError,
			SyncPointEnabled:  info.SyncPointEnabled,
			SyncPointInterval: info.SyncPointInterval,
			CreatorVersion:    info.CreatorVersion,
		},
		Status: &model.ChangeFeedStatus{
			ResolvedTs:   detail.ResolvedTs,
			CheckpointTs: detail.CheckpointTSO,
			AdminJobType: info.AdminJobType,
		},
	}
	return util.JSONPrint(cmd, meta)
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
