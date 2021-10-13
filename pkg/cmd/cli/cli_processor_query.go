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
	"github.com/pingcap/ticdc/pkg/cmd/factory"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/spf13/cobra"
)

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

// queryProcessorOptions defines flags for the `cli processor query` command.
type queryProcessorOptions struct {
	etcdClient *etcd.CDCEtcdClient

	changefeedID string
	captureID    string
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

// run runs the `cli processor query` command.
func (o *queryProcessorOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

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
