// Copyright 2022 PingCAP, Inc.
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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/enginepb"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// jobPauseOptions defines flags for job pause.
type jobPauseOptions struct {
	generalOpts *jobGeneralOptions

	jobID string
}

// newJobPauseOptions creates new pause job options.
func newJobPauseOptions(generalOpts *jobGeneralOptions) *jobPauseOptions {
	return &jobPauseOptions{generalOpts: generalOpts}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *jobPauseOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.Flags().StringVar(&o.jobID, "job-id", "", "job id")
}

func (o *jobPauseOptions) validate(ctx context.Context, cmd *cobra.Command) error {
	return o.generalOpts.validate(ctx, cmd)
}

// run the `cli job create` command.
func (o *jobPauseOptions) run(ctx context.Context, cmd *cobra.Command) error {
	resp, err := o.generalOpts.jobManagerCli.PauseJob(ctx, &enginepb.PauseJobRequest{
		JobId: o.jobID,
		ProjectInfo: &enginepb.ProjectInfo{
			TenantId:  o.generalOpts.tenant.TenantID(),
			ProjectId: o.generalOpts.tenant.ProjectID(),
		},
	})
	if err != nil {
		return err
	}
	log.Info("pause job request is sent", zap.Any("resp", resp))
	return nil
}

// newCmdJobPause creates the `cli job create` command.
func newCmdJobPause(generalOpts *jobGeneralOptions) *cobra.Command {
	o := newJobPauseOptions(generalOpts)

	command := &cobra.Command{
		Use:   "pause",
		Short: "Pause a job",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()
			if err := o.validate(ctx, cmd); err != nil {
				return err
			}
			return o.run(ctx, cmd)
		},
	}

	o.addFlags(command)

	return command
}
