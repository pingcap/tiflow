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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// listProcessorOptions defines flags for the `cli processor list` command.
type listProcessorOptions struct {
	etcdClient       *etcd.CDCEtcdClient
	apiClient        apiv1client.APIV1Interface
	runWithAPIClient bool
}

// newListProcessorOptions creates new listProcessorOptions for the `cli processor list` command.
func newListProcessorOptions() *listProcessorOptions {
	return &listProcessorOptions{}
}

// complete adapts from the command line args to the data and client required.
func (o *listProcessorOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient
	ctx := cmdcontext.GetDefaultContext()
	owner, err := getOwnerCapture(ctx, o.etcdClient)
	if err != nil {
		return err
	}

	o.apiClient, err = apiv1client.NewAPIClient(owner.AdvertiseAddr, f.GetCredential())
	if err != nil {
		return err
	}

	_, captureInfos, err := o.etcdClient.GetCaptures(ctx)
	if err != nil {
		return err
	}
	cdcClusterVer, err := version.GetTiCDCClusterVersion(model.ListVersionsFromCaptureInfos(captureInfos))
	if err != nil {
		return errors.Trace(err)
	}

	o.runWithAPIClient = true
	if !cdcClusterVer.ShouldRunCliWithAPIClientByDefault() {
		o.runWithAPIClient = false
		log.Warn("The TiCDC cluster is built from an older version, run cli with etcd client by default.",
			zap.String("version", cdcClusterVer.String()))
	}

	return nil
}

// run runs the `cli processor list` command.
func (o *listProcessorOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()
	if o.runWithAPIClient {
		processors, err := o.apiClient.Processors().List(ctx)
		if err != nil {
			return err
		}
		return util.JSONPrint(cmd, processors)
	}

	info, err := o.etcdClient.GetProcessors(ctx)
	if err != nil {
		return err
	}
	return util.JSONPrint(cmd, info)
}

// newCmdListProcessor creates the `cli processor list` command.
func newCmdListProcessor(f factory.Factory) *cobra.Command {
	o := newListProcessorOptions()

	command := &cobra.Command{
		Use:   "list",
		Short: "List all processors in TiCDC cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			err := o.complete(f)
			if err != nil {
				return err
			}

			return o.run(cmd)
		},
	}

	return command
}
