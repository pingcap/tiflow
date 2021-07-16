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

	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	cmdcontext "github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	pd "github.com/tikv/pd/client"
)

// resumeChangefeedOptions defines flags for the `cli changefeed resume` command.
type resumeChangefeedOptions struct {
	changefeedID string
	noConfirm    bool
}

// newResumeChangefeedOptions creates new options for the `cli changefeed pause` command.
func newResumeChangefeedOptions() *resumeChangefeedOptions {
	return &resumeChangefeedOptions{}
}

// newCmdResumeChangefeed creates the `cli changefeed resume` command.
func newCmdResumeChangefeed(f util.Factory) *cobra.Command {
	o := newResumeChangefeedOptions()

	command := &cobra.Command{
		Use:   "resume",
		Short: "Resume a paused replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()
			job := model.AdminJob{
				CfID: o.changefeedID,
				Type: model.AdminResume,
			}

			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			pdClient, err := f.PdClient()
			if err != nil {
				return err
			}

			credential := f.GetCredential()

			if err := resumeChangefeedCheck(ctx, etcdClient, pdClient, cmd, o.changefeedID, o.noConfirm, credential); err != nil {
				return err
			}

			return applyAdminChangefeed(ctx, etcdClient, job, credential)
		},
	}

	command.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	command.PersistentFlags().BoolVar(&o.noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")

	return command
}

func resumeChangefeedCheck(ctx context.Context, etcdClient *kv.CDCEtcdClient, pdClient pd.Client,
	cmd *cobra.Command, changefeedID string, noConfirm bool, credential *security.Credential) error {
	resp, err := applyOwnerChangefeedQuery(ctx, etcdClient, changefeedID, credential)
	if err != nil {
		return err
	}

	info := &cdc.ChangefeedResp{}
	err = json.Unmarshal([]byte(resp), info)
	if err != nil {
		return err
	}

	return confirmLargeDataGap(ctx, pdClient, cmd, noConfirm, info.TSO)
}
