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

	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
)

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

// queryProcessorOptions defines flags for the `cli processor query` command.
type queryProcessorOptions struct {
	apiClient apiv1client.APIV1Interface

	changefeedID string
	captureID    string
}

// newQueryProcessorOptions creates new options for the `cli changefeed query` command.
func newQueryProcessorOptions() *queryProcessorOptions {
	return &queryProcessorOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *queryProcessorOptions) complete(f factory.Factory) error {
	apiClient, err := f.APIV1Client()
	if err != nil {
		return err
	}
	o.apiClient = apiClient
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
	}

	return util.JSONPrint(cmd, meta)
}

// run runs the `cli processor query` command.
func (o *queryProcessorOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()
	return o.runCliWithAPIClient(ctx, cmd)
}

// newCmdQueryProcessor creates the `cli processor query` command.
func newCmdQueryProcessor(f factory.Factory) *cobra.Command {
	o := newQueryProcessorOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a sub replication task (processor)",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
