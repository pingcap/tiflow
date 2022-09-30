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
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	ticdcutil "github.com/pingcap/tiflow/pkg/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// cfMeta holds changefeed info and changefeed status.
type cfMeta struct {
	Info       *model.ChangeFeedInfo     `json:"info"`
	Status     *model.ChangeFeedStatus   `json:"status"`
	Count      uint64                    `json:"count"`
	TaskStatus []model.CaptureTaskStatus `json:"task-status"`
}

// queryChangefeedOptions defines flags for the `cli changefeed query` command.
type queryChangefeedOptions struct {
	etcdClient *etcd.CDCEtcdClient

	credential   *security.Credential
	apiClient    apiv1client.APIV1Interface
	changefeedID string
	simplified   bool
}

// newQueryChangefeedOptions creates new options for the `cli changefeed query` command.
func newQueryChangefeedOptions() *queryChangefeedOptions {
	return &queryChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *queryChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVarP(&o.simplified, "simple", "s", false, "Output simplified replication status")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *queryChangefeedOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	o.credential = f.GetCredential()

	ctx := cmdcontext.GetDefaultContext()
	owner, err := getOwnerCapture(ctx, o.etcdClient)
	if err != nil {
		return err
	}
	o.apiClient, err = apiv1client.NewAPIClient(owner.AdvertiseAddr, o.credential)
	if err != nil {
		return err
	}

	return nil
}

// run the `cli changefeed query` command.
func (o *queryChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	if o.simplified {
		resp, err := sendOwnerChangefeedQuery(ctx, o.etcdClient,
			model.DefaultChangeFeedID(o.changefeedID),
			o.credential)
		if err != nil {
			return err
		}

		cmd.Println(resp)

		return nil
	}

	info, err := o.etcdClient.GetChangeFeedInfo(ctx,
		model.DefaultChangeFeedID(o.changefeedID))
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}
	if info == nil {
		log.Warn("This changefeed has been deleted, the residual meta data will be completely deleted within 24 hours.", zap.String("changgefeed", o.changefeedID))
	}

	status, _, err := o.etcdClient.GetChangeFeedStatus(ctx,
		model.DefaultChangeFeedID(o.changefeedID))
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}

	if err != nil && cerror.ErrChangeFeedNotExists.Equal(err) {
		log.Error("This changefeed does not exist", zap.String("changefeed", o.changefeedID))
		return err
	}

	taskPositions, err := o.etcdClient.GetAllTaskPositions(ctx, o.changefeedID)
	if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
		return err
	}

	var count uint64
	for _, pinfo := range taskPositions {
		count += pinfo.Count
	}

	changefeedDetail, err := o.apiClient.Changefeeds().Get(ctx, o.changefeedID)
	if err != nil {
		return err
	}
	info.SinkURI, err = ticdcutil.MaskSinkURI(info.SinkURI)
	if err != nil {
		cmd.PrintErr(err)
	}
	meta := &cfMeta{Info: info, Status: status, Count: count, TaskStatus: changefeedDetail.TaskStatus}

	return util.JSONPrint(cmd, meta)
}

// newCmdQueryChangefeed creates the `cli changefeed query` command.
func newCmdQueryChangefeed(f factory.Factory) *cobra.Command {
	o := newQueryChangefeedOptions()

	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replication task (changefeed)",
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
