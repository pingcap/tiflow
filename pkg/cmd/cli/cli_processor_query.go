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
	"github.com/pingcap/ticdc/pkg/cmd/util"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
)

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

// queryProcessorOptions defines flags for the `cli processor query` command.
type queryProcessorOptions struct {
	changefeedID string
	captureID    string
}

// newQueryProcessorOptions creates new options for the `cli changefeed query` command.
func newQueryProcessorOptions() *queryProcessorOptions {
	return &queryProcessorOptions{}
}

// newCmdQueryProcessor creates the `cli processor query` command.
func newCmdQueryProcessor(f util.Factory) *cobra.Command {
	o := newQueryProcessorOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a sub replication task (processor)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()

			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			_, status, err := etcdClient.GetTaskStatus(ctx, o.changefeedID, o.captureID)
			if err != nil && cerror.ErrTaskStatusNotExists.Equal(err) {
				return err
			}

			_, position, err := etcdClient.GetTaskPosition(ctx, o.changefeedID, o.captureID)
			if err != nil && cerror.ErrTaskPositionNotExists.Equal(err) {
				return err
			}

			meta := &processorMeta{Status: status, Position: position}
			return util.JSONPrint(cmd, meta)
		},
	}

	command.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	command.PersistentFlags().StringVarP(&o.captureID, "capture-id", "p", "", "capture ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	_ = command.MarkPersistentFlagRequired("capture-id")

	return command
}
