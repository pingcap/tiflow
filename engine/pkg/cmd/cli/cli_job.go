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
	"time"

	"github.com/pingcap/log"
	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/engine/client"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
)

// jobGeneralOptions defines some general options of job management
type jobGeneralOptions struct {
	projectID string
	tenantID  string
	tenant    tenant.ProjectInfo

	masterClient client.MasterClient
	masterAddrs  []string
	rpcTimeout   time.Duration
	// TODO: add tls support
}

func newJobGeneralOptions() *jobGeneralOptions {
	return &jobGeneralOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *jobGeneralOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.PersistentFlags().StringVar(&o.tenantID, "tenant-id", "", "the tenant id")
	cmd.PersistentFlags().StringVar(&o.projectID, "project-id", "", "the project id")
	cmd.PersistentFlags().StringSliceVar(&o.masterAddrs, "master-addrs", nil, "server master addresses")
	cmd.PersistentFlags().DurationVar(&o.rpcTimeout, "rpc-timeout", time.Second*30, "default rpc timeout")
}

// validate checks that the provided job options are valid.
func (o *jobGeneralOptions) validate(ctx context.Context, cmd *cobra.Command) error {
	if len(o.masterAddrs) == 0 {
		return errors.ErrInvalidCliParameter.GenWithStack("master-addrs can't be nil")
	}

	ctx, cancel := context.WithTimeout(ctx, o.rpcTimeout)
	defer cancel()
	cliManager := client.NewClientManager()
	if err := cliManager.AddMasterClient(ctx, o.masterAddrs); err != nil {
		return err
	}
	o.masterClient = cliManager.MasterClient()

	o.tenant = o.getProjectInfo()

	return nil
}

func (o *jobGeneralOptions) getProjectInfo() tenant.ProjectInfo {
	var tenantID, projectID string
	if o.tenantID != "" {
		tenantID = o.tenantID
	} else {
		log.Warn("tenant-id is empty, use default tenant id")
		tenantID = tenant.DefaultUserProjectInfo.TenantID()
	}
	if o.projectID != "" {
		projectID = o.projectID
	} else {
		log.Warn("project-id is empty, use default project id")
		projectID = tenant.DefaultUserProjectInfo.ProjectID()
	}
	return tenant.NewProjectInfo(tenantID, projectID)
}

// newCmdJob creates the `cli job` command.
func newCmdJob() *cobra.Command {
	o := newJobGeneralOptions()

	cmds := &cobra.Command{
		Use:   "job",
		Short: "Manage job",
		Args:  cobra.NoArgs,
	}

	o.addFlags(cmds)
	cmds.AddCommand(newCmdCreateJob(o))
	cmds.AddCommand(newCmdQueryJob(o))
	cmds.AddCommand(newCmdPauseJob(o))

	return cmds
}
