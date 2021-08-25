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
	"fmt"
	"io/ioutil"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/factory"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
)

// newCmdChangefeed creates the `cli changefeed` command.
func newCmdChangefeed(f factory.Factory) *cobra.Command {
	cmds := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
	}

	cmds.AddCommand(newCmdListChangefeed(f))
	cmds.AddCommand(newCmdQueryChangefeed(f))
	cmds.AddCommand(newCmdCreateChangefeed(f))
	cmds.AddCommand(newCmdUpdateChangefeed(f))
	cmds.AddCommand(newCmdStatisticsChangefeed(f))
	cmds.AddCommand(newCmdCyclicChangefeed(f))
	cmds.AddCommand(newCmdPauseChangefeed(f))
	cmds.AddCommand(newCmdResumeChangefeed(f))
	cmds.AddCommand(newCmdRemoveChangefeed(f))

	return cmds
}

// sendOwnerAdminChangeQuery sends owner admin query request.
func sendOwnerAdminChangeQuery(ctx context.Context, etcdClient *kv.CDCEtcdClient, job model.AdminJob, credential *security.Credential) error {
	owner, err := getOwnerCapture(ctx, etcdClient)
	if err != nil {
		return err
	}

	scheme := util.HTTP
	if credential.IsTLSEnabled() {
		scheme = util.HTTPS
	}

	url := fmt.Sprintf("%s://%s/capture/owner/admin", scheme, owner.AdvertiseAddr)
	httpClient, err := httputil.NewClient(credential)
	if err != nil {
		return err
	}

	forceRemoveOpt := "false"
	if job.Opts != nil && job.Opts.ForceRemove {
		forceRemoveOpt = "true"
	}

	resp, err := httpClient.PostForm(url, map[string][]string{
		cdc.APIOpVarAdminJob:           {fmt.Sprint(int(job.Type))},
		cdc.APIOpVarChangefeedID:       {job.CfID},
		cdc.APIOpForceRemoveChangefeed: {forceRemoveOpt},
	})
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return errors.BadRequestf("admin changefeed failed")
		}
		return errors.BadRequestf("%s", string(body))
	}

	return nil
}
