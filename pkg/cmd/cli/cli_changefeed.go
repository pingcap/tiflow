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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

const (
	// tsGapWarning specifies the OOM threshold.
	// 1 day in milliseconds
	tsGapWarning = 86400 * 1000
)

// changefeedCommonOptions defines common flags for the `changefeed` command.
type changefeedCommonOptions struct {
	changefeedID string
	NoConfirm    bool
}

// newChangefeedCommonOptions creates new common options for the `changefeed` command.
func newChangefeedCommonOptions() *changefeedCommonOptions {
	return &changefeedCommonOptions{}
}

// newCmdChangefeed creates the `cli changefeed` command.
func newCmdChangefeed(f util.Factory, options *options) *cobra.Command {
	changefeedCommonOptions := newChangefeedCommonOptions()

	cmds := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
	}

	cmds.AddCommand(newCmdListChangefeed(f))
	cmds.AddCommand(newCmdQueryChangefeed(f, changefeedCommonOptions))
	cmds.AddCommand(newCmdPauseChangefeed(f, changefeedCommonOptions))
	cmds.AddCommand(newCmdResumeChangefeed(f, changefeedCommonOptions))
	cmds.AddCommand(newCmdRemoveChangefeed(f, changefeedCommonOptions))
	cmds.AddCommand(newCmdCreateChangefeed(f, options, changefeedCommonOptions))
	cmds.AddCommand(newCmdUpdateChangefeed(f, options, changefeedCommonOptions))
	cmds.AddCommand(newCmdStatisticsChangefeed(f, changefeedCommonOptions))
	cmds.AddCommand(newCmdCyclicChangefeed(f))

	cmds.PersistentFlags().StringVarP(&changefeedCommonOptions.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmds.PersistentFlags().BoolVar(&changefeedCommonOptions.NoConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")

	return cmds
}

func applyOwnerChangefeedQuery(ctx context.Context, etcdClient *kv.CDCEtcdClient,
	cid model.ChangeFeedID, credential *security.Credential,
) (string, error) {
	owner, err := getOwnerCapture(ctx, etcdClient)
	if err != nil {
		return "", err
	}

	scheme := util.HTTP
	if credential.IsTLSEnabled() {
		scheme = util.HTTPS
	}

	addr := fmt.Sprintf("%s://%s/capture/owner/changefeed/query", scheme, owner.AdvertiseAddr)
	cli, err := httputil.NewClient(credential)
	if err != nil {
		return "", err
	}
	resp, err := cli.PostForm(addr, map[string][]string{
		cdc.APIOpVarChangefeedID: {cid},
	})
	if err != nil {
		return "", err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", errors.BadRequestf("query changefeed simplified status")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", errors.BadRequestf("%s", string(body))
	}

	return string(body), nil
}

func applyAdminChangefeed(ctx context.Context, etcdClient *kv.CDCEtcdClient, job model.AdminJob, credential *security.Credential) error {
	owner, err := getOwnerCapture(ctx, etcdClient)
	if err != nil {
		return err
	}

	scheme := util.HTTP
	if credential.IsTLSEnabled() {
		scheme = util.HTTPS
	}

	addr := fmt.Sprintf("%s://%s/capture/owner/admin", scheme, owner.AdvertiseAddr)
	cli, err := httputil.NewClient(credential)
	if err != nil {
		return err
	}
	forceRemoveOpt := "false"
	if job.Opts != nil && job.Opts.ForceRemove {
		forceRemoveOpt = "true"
	}
	resp, err := cli.PostForm(addr, map[string][]string{
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

func confirmLargeDataGap(ctx context.Context, pdClient pd.Client, cmd *cobra.Command, commonOptions *changefeedCommonOptions, startTs uint64) error {
	if commonOptions.NoConfirm {
		return nil
	}

	currentPhysical, _, err := pdClient.GetTS(ctx)
	if err != nil {
		return err
	}

	tsGap := currentPhysical - oracle.ExtractPhysical(startTs)
	if tsGap > tsGapWarning {
		cmd.Printf("Replicate lag (%s) is larger than 1 days, "+
			"large data may cause OOM, confirm to continue at your own risk [Y/N]\n",
			time.Duration(tsGap)*time.Millisecond,
		)
		var yOrN string
		_, err := fmt.Scan(&yOrN)
		if err != nil {
			return err
		}
		if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
			return errors.NewNoStackError("abort changefeed create or resume")
		}
	}

	return nil
}
