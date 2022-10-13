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
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
)

// cfMeta holds changefeed info and changefeed status.
type cfMeta struct {
	UpstreamID     uint64                    `json:"upstream_id"`
	Namespace      string                    `json:"namespace"`
	ID             string                    `json:"id"`
	SinkURI        string                    `json:"sink_uri"`
	Config         *v2.ReplicaConfig         `json:"config"`
	CreateTime     model.JSONTime            `json:"create_time"`
	StartTs        uint64                    `json:"start_ts"`
	ResolvedTs     uint64                    `json:"resolved_ts"`
	TargetTs       uint64                    `json:"target_ts"`
	CheckpointTSO  uint64                    `json:"checkpoint_tso"`
	CheckpointTime model.JSONTime            `json:"checkpoint_time"`
	Engine         model.SortEngine          `json:"sort_engine,omitempty"`
	FeedState      model.FeedState           `json:"state"`
	RunningError   *model.RunningError       `json:"error"`
	ErrorHis       []int64                   `json:"error_history"`
	CreatorVersion string                    `json:"creator_version"`
	TaskStatus     []model.CaptureTaskStatus `json:"task_status,omitempty"`
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
	meta := &cfMeta{
		UpstreamID:     detail.UpstreamID,
		Namespace:      detail.Namespace,
		ID:             detail.ID,
		SinkURI:        detail.SinkURI,
		Config:         info.Config,
		CreateTime:     detail.CreateTime,
		StartTs:        detail.StartTs,
		ResolvedTs:     detail.ResolvedTs,
		TargetTs:       detail.TargetTs,
		CheckpointTSO:  detail.CheckpointTSO,
		CheckpointTime: detail.CheckpointTime,
		Engine:         detail.Engine,
		FeedState:      detail.FeedState,
		RunningError:   detail.RunningError,
		ErrorHis:       detail.ErrorHis,
		CreatorVersion: detail.CreatorVersion,
		TaskStatus:     detail.TaskStatus,
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
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
