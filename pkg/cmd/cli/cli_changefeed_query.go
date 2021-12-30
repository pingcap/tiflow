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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// changeFeedMeta holds changefeed info and changefeed status.
type changeFeedMeta struct {
	Info       *model.ChangeFeedInfo     `json:"info"`
	Status     *model.ChangeFeedStatus   `json:"status"`
	Count      uint64                    `json:"count"`
	TaskStatus []model.CaptureTaskStatus `json:"task-status"`
}

// queryChangefeedOptions defines flags for the `cli changefeed query` command.
type queryChangefeedOptions struct {
	etcdClient *etcd.CDCEtcdClient
	apiClient  apiv1client.APIV1Interface

	credential *security.Credential

	changefeedID     string
	simplified       bool
	runWithAPIClient bool
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
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient
	ctx := cmdcontext.GetDefaultContext()

	owner, err := getOwnerCapture(ctx, o.etcdClient)
	if err != nil {
		return err
	}

	o.credential = f.GetCredential()
	o.apiClient, err = apiv1client.NewAPIClient(owner.AdvertiseAddr, o.credential)
	if err != nil {
		return err
	}

	_, captureInfos, err := o.etcdClient.GetCaptures(ctx)
	if err != nil {
		return err
	}
	cdcClusterVer, err := version.GetTiCDCClusterVersion(model.ListVersionsFromCaptureInfos(captureInfos))
	if err != nil {
		return errors.Trace(err)
	}

	o.runWithAPIClient = true
	if !cdcClusterVer.ShouldRunCliWithAPIClientByDefault() {
		o.runWithAPIClient = false
		log.Warn("The TiCDC cluster is built from an older version, run cli with etcd client by default.",
			zap.String("version", cdcClusterVer.String()))
	}

	return nil
}

// run cli command with etcd client
func (o *queryChangefeedOptions) runCliWithEtcdClient(ctx context.Context, cmd *cobra.Command) error {
	info, err := o.etcdClient.GetChangeFeedInfo(ctx, o.changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}
	if info == nil {
		log.Warn("This changefeed has been deleted, the residual meta data will be completely deleted within 24 hours.", zap.String("changgefeed", o.changefeedID))
	}

	status, _, err := o.etcdClient.GetChangeFeedStatus(ctx, o.changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}

	if err != nil && cerror.ErrChangeFeedNotExists.Equal(err) {
		log.Error("This changefeed does not exist", zap.String("changefeed", o.changefeedID))
		return err
	}

	taskPositions, err := o.etcdClient.GetAllTaskPositions(ctx, o.changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}

	var count uint64
	for _, pinfo := range taskPositions {
		count += pinfo.Count
	}

	processorInfos, err := o.etcdClient.GetAllTaskStatus(ctx, o.changefeedID)
	if err != nil {
		return err
	}

	taskStatus := make([]model.CaptureTaskStatus, 0, len(processorInfos))
	for captureID, status := range processorInfos {
		taskStatus = append(taskStatus, model.CaptureTaskStatus{
			CaptureID: captureID,
			Tables:    status.Tables,
			Operation: status.Operation,
		})
	}

	meta := &changeFeedMeta{Info: info, Status: status, Count: count, TaskStatus: taskStatus}
	return util.JSONPrint(cmd, meta)
}

// run cli command with the encapsulated api client
func (o *queryChangefeedOptions) runCliWithAPIClient(ctx context.Context, cmd *cobra.Command) error {
	var count uint64
	// count cannot be properly calculated until https://github.com/pingcap/tiflow/pull/4052 is merged

	// captures, err := o.apiClient.Captures().List(ctx)
	// if err != nil {
	// 	return err
	// }

	// for _, capture := range *captures {
	// 	processor, err := o.apiClient.Processors().Get(ctx, o.changefeedID, capture.ID)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	count += processor.Count
	// }

	changefeed, err := o.apiClient.Changefeeds().Get(ctx, o.changefeedID)
	if err != nil {
		return err
	}

	meta := &changeFeedMeta{
		Info: &model.ChangeFeedInfo{
			SinkURI: changefeed.SinkURI,
			// Opts is vacant
			CreateTime: time.Time(changefeed.CreateTime),
			StartTs:    changefeed.StartTs,
			TargetTs:   changefeed.TargetTs,
			// AdminJobType is vacant
			Engine: changefeed.Engine,
			// SortDir is vacant
			// Config is vacant
			State:    changefeed.FeedState,
			ErrorHis: changefeed.ErrorHis,
			Error:    changefeed.RunningError,
			// SyncPointEnabled is vacant
			// SyncPointInterval is vacant
			// CreatorVersion is vacant
		},
		Status: &model.ChangeFeedStatus{
			ResolvedTs:   changefeed.ResolvedTs,
			CheckpointTs: changefeed.CheckpointTSO,
		},
		Count:      count,
		TaskStatus: changefeed.TaskStatus,
	}

	return util.JSONPrint(cmd, meta)
}

// run the `cli changefeed query` command.
func (o *queryChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	if o.simplified {
		resp, err := sendOwnerChangefeedQuery(ctx, o.etcdClient, o.changefeedID, o.credential)
		if err != nil {
			return err
		}

		cmd.Println(resp)

		return nil
	}

	if o.runWithAPIClient {
		return o.runCliWithAPIClient(ctx, cmd)
	}

	return o.runCliWithEtcdClient(ctx, cmd)
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
