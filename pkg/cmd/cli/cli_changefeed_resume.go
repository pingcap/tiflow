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
	"encoding/json"

	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/model"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
)

// resumeChangefeedOptions defines flags for the `cli changefeed resume` command.
type resumeChangefeedOptions struct {
	etcdClient *etcd.CDCEtcdClient
	pdClient   pd.Client

	credential *security.Credential

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
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}

	o.pdClient = pdClient

	o.credential = f.GetCredential()

	return nil
}

// confirmResumeChangefeedCheck prompts the user to confirm the use of a large data gap when noConfirm is turned off.
func (o *resumeChangefeedOptions) confirmResumeChangefeedCheck(ctx context.Context, cmd *cobra.Command) error {
	resp, err := sendOwnerChangefeedQuery(ctx, o.etcdClient,
		model.DefaultChangeFeedID(o.changefeedID),
		o.credential)
	if err != nil {
		return err
	}

	info := &api.ChangefeedResp{}
	err = json.Unmarshal([]byte(resp), info)
	if err != nil {
		return err
	}

	currentPhysical, _, err := o.pdClient.GetTS(ctx)
	if err != nil {
		return err
	}

	if !o.noConfirm {
		return confirmLargeDataGap(cmd, currentPhysical, info.TSO)
	}

	return nil
}

// run the `cli changefeed resume` command.
func (o *resumeChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	if err := o.confirmResumeChangefeedCheck(ctx, cmd); err != nil {
		return err
	}

	job := model.AdminJob{
		CfID: model.DefaultChangeFeedID(o.changefeedID),
		Type: model.AdminResume,
	}

	return sendOwnerAdminChangeQuery(ctx, o.etcdClient, job, o.credential)
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
