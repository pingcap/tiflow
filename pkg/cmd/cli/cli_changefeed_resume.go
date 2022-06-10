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

	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
)

// resumeChangefeedOptions defines flags for the `cli changefeed resume` command.
type resumeChangefeedOptions struct {
	apiV1Client *apiv1client.APIV1Client
	apiV2Client *apiv2client.APIV2Client

	changefeedID string
	noConfirm    bool
}

// newResumeChangefeedOptions creates new options for the `cli changefeed pause` command.
func newResumeChangefeedOptions() *resumeChangefeedOptions {
	return &resumeChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *resumeChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().BoolVar(&o.noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *resumeChangefeedOptions) complete(f factory.Factory) error {
	apiClient, err := f.APIV1Client()
	if err != nil {
		return err
	}
	o.apiV1Client = apiClient
	apiClient2, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiV2Client = apiClient2
	return nil
}

// confirmResumeChangefeedCheck prompts the user to confirm the use of a large data gap when noConfirm is turned off.
func (o *resumeChangefeedOptions) confirmResumeChangefeedCheck(ctx context.Context, cmd *cobra.Command) error {
	if !o.noConfirm {
		cfs, err := o.apiV1Client.Changefeeds().List(ctx)
		if err != nil {
			return err
		}
		var checkpointTs uint64 = 0

		for _, cf := range *cfs {
			if cf.ID == o.changefeedID {
				checkpointTs = cf.CheckpointTSO
			}
		}
		if checkpointTs == 0 {
			return errors.ErrChangeFeedNotExists
		}

		tso, err := o.apiV2Client.Tso().Get(ctx)
		if err != nil {
			return err
		}
		return confirmLargeDataGap(cmd, tso.Timestamp, checkpointTs)
	}
	return nil
}

// run the `cli changefeed resume` command.
func (o *resumeChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	if err := o.confirmResumeChangefeedCheck(ctx, cmd); err != nil {
		return err
	}
	return o.apiV1Client.Changefeeds().Resume(ctx, o.changefeedID)
}

// newCmdResumeChangefeed creates the `cli changefeed resume` command.
func newCmdResumeChangefeed(f factory.Factory) *cobra.Command {
	o := newResumeChangefeedOptions()

	command := &cobra.Command{
		Use:   "resume",
		Short: "Resume a paused replication task (changefeed)",
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
