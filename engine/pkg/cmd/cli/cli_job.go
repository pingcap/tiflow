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
	"github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// defaultMasterAddr is the default master address.
const defaultMasterAddr = "127.0.0.1:10240"

// jobGeneralOptions defines some general options of job management
type jobGeneralOptions struct {
	projectID string
	tenantID  string

	masterAddrs []string
	rpcTimeout  time.Duration
	// TODO: add tls support

	// Following fields are generated from options
	jobManagerCli enginepb.JobManagerClient
	tenant        tenant.ProjectInfo
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
func (o *jobGeneralOptions) validate(_ context.Context) error {
	if len(o.masterAddrs) == 0 {
		o.masterAddrs = []string{defaultMasterAddr}
		log.Warn("the master-addrs are not assigned, use default addr: " + defaultMasterAddr)
	}

	// TODO support https.
	dialURL := o.masterAddrs[0]
	grpcConn, err := grpc.Dial(dialURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return errors.Trace(err)
	}
	o.jobManagerCli = enginepb.NewJobManagerClient(grpcConn)
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
	cmds.AddCommand(newCmdJobCreate(o))
	cmds.AddCommand(newCmdJobQuery(o))
	cmds.AddCommand(newCmdJobCancel(o))

	return cmds
}
