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

// newCmdResumeChangefeed creates the `cli changefeed resume` command.
func newCmdResumeChangefeed(f util.Factory, commonOptions *changefeedCommonOptions) *cobra.Command {
	command := &cobra.Command{
		Use:   "resume",
		Short: "Resume a paused replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()
			job := model.AdminJob{
				CfID: commonOptions.changefeedID,
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

			if err := resumeChangefeedCheck(ctx, etcdClient, pdClient, cmd, commonOptions, credential); err != nil {
				return err
			}

			return applyAdminChangefeed(ctx, etcdClient, job, credential)
		},
	}

	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}

func resumeChangefeedCheck(ctx context.Context, etcdClient *kv.CDCEtcdClient, pdClient pd.Client,
	cmd *cobra.Command, commonOptions *changefeedCommonOptions, credential *security.Credential) error {
	resp, err := applyOwnerChangefeedQuery(ctx, etcdClient, commonOptions.changefeedID, credential)
	if err != nil {
		return err
	}

	info := &cdc.ChangefeedResp{}
	err = json.Unmarshal([]byte(resp), info)
	if err != nil {
		return err
	}

	return confirmLargeDataGap(ctx, pdClient, cmd, commonOptions, info.TSO)
}
