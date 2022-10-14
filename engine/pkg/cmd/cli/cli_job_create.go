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
	"fmt"
	"io"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/enginepb"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// jobCreateOptions defines flags for job create.
type jobCreateOptions struct {
	generalOpts *jobGeneralOptions

	jobConfigStr string

	jobID     string
	jobType   enginepb.Job_Type
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

	cmd.Flags().Var(newJobTypeValue(enginepb.Job_TypeUnknown, &o.jobType), "job-type", "job type, one of [FakeJob, CVSDemo, DM]")
	cmd.Flags().StringVar(&o.jobConfigStr, "job-config", "", "path of config file for the job")
	cmd.Flags().StringVar(&o.jobID, "job-id", "", "job id")

	_ = cmd.MarkFlagRequired("job-type")
}

type jobTypeValue enginepb.Job_Type

func newJobTypeValue(jobType enginepb.Job_Type, p *enginepb.Job_Type) *jobTypeValue {
	*p = jobType
	return (*jobTypeValue)(p)
}

func (v *jobTypeValue) String() string {
	if enginepb.Job_Type(*v) == enginepb.Job_TypeUnknown {
		return ""
	}
	return enginepb.Job_Type(*v).String()
}

func (v *jobTypeValue) Set(val string) error {
	switch val {
	case "FakeJob":
		*v = jobTypeValue(enginepb.Job_FakeJob)
	case "CVSDemo":
		*v = jobTypeValue(enginepb.Job_CVSDemo)
	case "DM":
		*v = jobTypeValue(enginepb.Job_DM)
	default:
		return fmt.Errorf("job type must be one of [FakeJob, CVSDemo, DM]")
	}
	return nil
}

func (v *jobTypeValue) Type() string {
	return "job-type"
}

// validate checks that the provided job options are valid.
func (o *jobCreateOptions) validate(ctx context.Context) error {
	if err := o.generalOpts.validate(ctx); err != nil {
		return errors.WrapError(errors.ErrInvalidCliParameter, err)
	}

	jobConfig, err := openFileAndReadString(o.jobConfigStr)
	if err != nil {
		return errors.WrapError(errors.ErrInvalidCliParameter, err)
	}
	o.jobConfig = jobConfig

	return nil
}

func openFileAndReadString(path string) (content []byte, err error) {
	if path == "" {
		log.Warn("create job with empty config file")
		return nil, nil
	}
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	return io.ReadAll(fp)
}

// run the `cli job create` command.
func (o *jobCreateOptions) run(ctx context.Context) error {
	job, err := o.generalOpts.jobManagerCli.CreateJob(ctx, &enginepb.CreateJobRequest{
		Job: &enginepb.Job{
			Type:   o.jobType,
			Config: o.jobConfig,
		},
		TenantId:  o.generalOpts.tenant.TenantID(),
		ProjectId: o.generalOpts.tenant.ProjectID(),
	})
	if err != nil {
		return err
	}
	log.Info("create job successfully", zap.Any("job", job))
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
			if err := o.validate(ctx); err != nil {
				return err
			}

			return o.run(ctx)
		},
	}

	o.addFlags(command)

	return command
}
