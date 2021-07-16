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
	"github.com/spf13/cobra"
)

// pauseChangefeedOptions defines flags for the `cli changefeed pause` command.
type pauseChangefeedOptions struct {
	changefeedID string
}

// newPauseChangefeedOptions creates new options for the `cli changefeed pause` command.
func newPauseChangefeedOptions() *pauseChangefeedOptions {
	return &pauseChangefeedOptions{}
}

// newCmdPauseChangefeed creates the `cli changefeed pause` command.
func newCmdPauseChangefeed(f util.Factory) *cobra.Command {
	o := newPauseChangefeedOptions()

	command := &cobra.Command{
		Use:   "pause",
		Short: "Pause a replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			job := model.AdminJob{
				CfID: o.changefeedID,
				Type: model.AdminStop,
			}

			ctx := context.GetDefaultContext()

			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			return applyAdminChangefeed(ctx, etcdClient, job, f.GetCredential())
		},
	}

	command.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}
