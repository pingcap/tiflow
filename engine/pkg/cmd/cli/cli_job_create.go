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
	"io"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/enginepb"
	engineModel "github.com/pingcap/tiflow/engine/model"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// jobCreateOptions defines flags for job create.
type jobCreateOptions struct {
	generalOpts *jobGeneralOptions

	jobTypeStr   string
	jobConfigStr string

	jobType   engineModel.JobType
	jobConfig []byte
}

// newJobCreateOptions creates new job options.
func newJobCreateOptions(generalOpts *jobGeneralOptions) *jobCreateOptions {
	return &jobCreateOptions{generalOpts: generalOpts}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *jobCreateOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.Flags().StringVar(&o.jobTypeStr, "job-type", "", "job type")
	cmd.Flags().StringVar(&o.jobConfigStr, "job-config", "", "path of config file for the job")
}

// validate checks that the provided job options are valid.
func (o *jobCreateOptions) validate(ctx context.Context, cmd *cobra.Command) error {
	if err := o.generalOpts.validate(ctx, cmd); err != nil {
		return errors.WrapError(errors.ErrInvalidCliParameter, err)
	}

	jobType, ok := engineModel.GetJobTypeByName(o.jobTypeStr)
	if !ok {
		return errors.ErrInvalidJobType.GenWithStackByArgs(o.jobType)
	}

	jobConfig, err := openFileAndReadString(o.jobConfigStr)
	if err != nil {
		return errors.WrapError(errors.ErrInvalidCliParameter, err)
	}

	o.jobType = jobType
	o.jobConfig = jobConfig

	return nil
}

func openFileAndReadString(path string) (content []byte, err error) {
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	return io.ReadAll(fp)
}

// run the `cli job create` command.
func (o *jobCreateOptions) run(ctx context.Context, cmd *cobra.Command) error {
	resp, err := o.generalOpts.masterClient.SubmitJob(ctx, &enginepb.SubmitJobRequest{
		Tp:     int32(o.jobType),
		Config: o.jobConfig,
		ProjectInfo: &enginepb.ProjectInfo{
			TenantId:  o.generalOpts.tenant.TenantID(),
			ProjectId: o.generalOpts.tenant.ProjectID(),
		},
	})
	if err != nil {
		return err
	}
	log.L().Info("create job successfully", zap.Any("resp", resp))
	return nil
}

// newCmdJobCreate creates the `cli job create` command.
func newCmdJobCreate(generalOpts *jobGeneralOptions) *cobra.Command {
	o := newJobCreateOptions(generalOpts)

	command := &cobra.Command{
		Use:   "create",
		Short: "Create a new job",
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
