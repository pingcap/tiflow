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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

// queryProcessorOptions defines flags for the `cli processor query` command.
type queryProcessorOptions struct {
	etcdClient *etcd.CDCEtcdClient
	apiClient  apiv1client.APIV1Interface

	changefeedID     string
	captureID        string
	runWithAPIClient bool
}

// newQueryProcessorOptions creates new options for the `cli changefeed query` command.
func newQueryProcessorOptions() *queryProcessorOptions {
	return &queryProcessorOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *queryProcessorOptions) complete(f factory.Factory) error {
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

	o.apiClient, err = apiv1client.NewAPIClient(owner.AdvertiseAddr, f.GetCredential())
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

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *queryProcessorOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().StringVarP(&o.captureID, "capture-id", "p", "", "capture ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
	_ = cmd.MarkPersistentFlagRequired("capture-id")
}

// run cli cmd with etcd client
func (o *queryProcessorOptions) runCliWithEtcdClient(ctx context.Context, cmd *cobra.Command) error {
	_, status, err := o.etcdClient.GetTaskStatus(ctx, o.changefeedID, o.captureID)
	if err != nil && cerror.ErrTaskStatusNotExists.Equal(err) {
		return err
	}

	_, position, err := o.etcdClient.GetTaskPosition(ctx, o.changefeedID, o.captureID)
	if err != nil && cerror.ErrTaskPositionNotExists.Equal(err) {
		return err
	}

	meta := &processorMeta{Status: status, Position: position}

	return util.JSONPrint(cmd, meta)
}

// run cli cmd with api client
func (o *queryProcessorOptions) runCliWithAPIClient(ctx context.Context, cmd *cobra.Command) error {
	processor, err := o.apiClient.Processors().Get(ctx, o.changefeedID, o.captureID)
	if err != nil {
		return err
	}

	tables := make(map[int64]*model.TableReplicaInfo)
	for _, tableID := range processor.Tables {
		tables[tableID] = &model.TableReplicaInfo{
			// to be compatible with old version `cli processor query`,
			// set this field to 0
			StartTs: 0,
		}
	}

	meta := &processorMeta{
		Status: &model.TaskStatus{
			Tables: tables,
			// Operations, AdminJobType and ModRevision are vacant
		},
		Position: &model.TaskPosition{
			CheckPointTs: processor.CheckPointTs,
			ResolvedTs:   processor.ResolvedTs,
			Count:        processor.Count,
			Error:        processor.Error,
		},
	}

	return util.JSONPrint(cmd, meta)
}

// run runs the `cli processor query` command.
func (o *queryProcessorOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()
	if o.runWithAPIClient {
		return o.runCliWithAPIClient(ctx, cmd)
	}

	return o.runCliWithEtcdClient(ctx, cmd)
}

// newCmdQueryProcessor creates the `cli processor query` command.
func newCmdQueryProcessor(f factory.Factory) *cobra.Command {
	o := newQueryProcessorOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a sub replication task (processor)",
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
