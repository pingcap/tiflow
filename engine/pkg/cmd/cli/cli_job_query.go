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

// jobQueryOptions defines flags for job query.
type jobQueryOptions struct {
	generalOpts *jobGeneralOptions

	jobID string
}

// newJobQueryOptions creates new query job options.
func newJobQueryOptions(generalOpts *jobGeneralOptions) *jobQueryOptions {
	return &jobQueryOptions{generalOpts: generalOpts}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *jobQueryOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.Flags().StringVar(&o.jobID, "job-id", "", "job id")
}

func (o *jobQueryOptions) validate(ctx context.Context) error {
	return o.generalOpts.validate(ctx)
}

// run the `cli job create` command.
func (o *jobQueryOptions) run(ctx context.Context) error {
	resp, err := o.generalOpts.jobManagerCli.GetJob(ctx, &enginepb.GetJobRequest{
		Id:        o.jobID,
		TenantId:  o.generalOpts.tenant.TenantID(),
		ProjectId: o.generalOpts.tenant.ProjectID(),
	})
	if err != nil {
		return err
	}
	log.Info("query job successfully", zap.Any("resp", resp))
	return nil
}

// newCmdJobQuery creates the `cli job create` command.
func newCmdJobQuery(generalOpts *jobGeneralOptions) *cobra.Command {
	o := newJobQueryOptions(generalOpts)

	command := &cobra.Command{
		Use:   "query",
		Short: "Query a job",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()
			if err := o.validate(ctx); err != nil {
				return err
			}
			return o.run(ctx)
		},
	}

	o.addFlags(command)

	return command
}
