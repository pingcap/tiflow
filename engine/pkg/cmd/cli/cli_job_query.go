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

// queryJobOptions defines flags for job query.
type queryJobOptions struct {
	generalOpts *jobGeneralOptions

	jobID string
}

// newQueryJobOptions creates new query job options.
func newQueryJobOptions(generalOpts *jobGeneralOptions) *queryJobOptions {
	return &queryJobOptions{generalOpts: generalOpts}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *queryJobOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.Flags().StringVar(&o.jobID, "job-id", "", "job id")
}

// run the `cli job create` command.
func (o *queryJobOptions) run(ctx context.Context, cmd *cobra.Command) error {
	resp, err := o.generalOpts.masterClient.QueryJob(ctx, &enginepb.QueryJobRequest{
		JobId: o.jobID,
		ProjectInfo: &enginepb.ProjectInfo{
			TenantId:  o.generalOpts.tenant.TenantID(),
			ProjectId: o.generalOpts.tenant.ProjectID(),
		},
	})
	if err != nil {
		return err
	}
	log.L().Info("query job successfully", zap.Any("resp", resp))
	return nil
}

// newCmdQueryJob creates the `cli job create` command.
func newCmdQueryJob(generalOpts *jobGeneralOptions) *cobra.Command {
	o := newQueryJobOptions(generalOpts)

	command := &cobra.Command{
		Use:   "query",
		Short: "Query a job",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()
			return o.run(ctx, cmd)
		},
	}

	o.addFlags(command)

	return command
}
