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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// captureTaskStatus holds capture task status.
type captureTaskStatus struct {
	CaptureID  string            `json:"capture-id"`
	TaskStatus *model.TaskStatus `json:"status"`
}

// cfMeta holds changefeed info and changefeed status.
type cfMeta struct {
	Info       *model.ChangeFeedInfo   `json:"info"`
	Status     *model.ChangeFeedStatus `json:"status"`
	Count      uint64                  `json:"count"`
	TaskStatus []captureTaskStatus     `json:"task-status"`
}

// queryChangefeedOptions defines flags for the `cli changefeed query` command.
type queryChangefeedOptions struct {
	changefeedID string
	simplified   bool
}

// newQueryChangefeedOptions creates new options for the `cli changefeed query` command.
func newQueryChangefeedOptions() *queryChangefeedOptions {
	return &queryChangefeedOptions{}
}

// newCmdQueryChangefeed creates the `cli changefeed query` command.
func newCmdQueryChangefeed(f util.Factory) *cobra.Command {
	o := newQueryChangefeedOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()

			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			if o.simplified {
				resp, err := applyOwnerChangefeedQuery(ctx, etcdClient, o.changefeedID, f.GetCredential())
				if err != nil {
					return err
				}
				cmd.Println(resp)
				return nil
			}

			info, err := etcdClient.GetChangeFeedInfo(ctx, o.changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}

			status, _, err := etcdClient.GetChangeFeedStatus(ctx, o.changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}
			if err != nil && cerror.ErrChangeFeedNotExists.Equal(err) {
				log.Error("This changefeed does not exist", zap.String("changefeed", o.changefeedID))
				return err
			}

			taskPositions, err := etcdClient.GetAllTaskPositions(ctx, o.changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}
			var count uint64
			for _, pinfo := range taskPositions {
				count += pinfo.Count
			}

			processorInfos, err := etcdClient.GetAllTaskStatus(ctx, o.changefeedID)
			if err != nil {
				return err
			}
			taskStatus := make([]captureTaskStatus, 0, len(processorInfos))
			for captureID, status := range processorInfos {
				taskStatus = append(taskStatus, captureTaskStatus{CaptureID: captureID, TaskStatus: status})
			}

			meta := &cfMeta{Info: info, Status: status, Count: count, TaskStatus: taskStatus}
			if info == nil {
				log.Warn("This changefeed has been deleted, the residual meta data will be completely deleted within 24 hours.", zap.String("changgefeed", o.changefeedID))
			}

			return util.JSONPrint(cmd, meta)
		},
	}

	command.PersistentFlags().BoolVarP(&o.simplified, "simple", "s", false, "Output simplified replication status")
	command.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}
