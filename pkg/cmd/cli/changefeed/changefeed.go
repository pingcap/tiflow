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

package changefeed

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
	"github.com/pingcap/ticdc/pkg/cmd/cli/capture"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	// tsGapWarning specifies the OOM threshold.
	// 1 day in milliseconds
	tsGapWarning = 86400 * 1000
)

// commonOptions defines common flags for the `changefeed` command.
type commonOptions struct {
	changefeedID string
	NoConfirm    bool
}

// newCommonOptions creates new common options for the `changefeed` command.
func newCommonOptions() *commonOptions {
	return &commonOptions{}
}

// NewCmdChangefeed creates the `cli changefeed` command.
func NewCmdChangefeed(f util.Factory) *cobra.Command {
	o := newCommonOptions()

	cmds := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
	}

	cmds.AddCommand(NewCmdListChangefeeds(f))
	cmds.AddCommand(NewCmdQueryChangefeed(f, o))
	cmds.AddCommand(NewCmdPauseChangefeed(f, o))
	cmds.AddCommand(NewCmdResumeChangefeed(f, o))
	cmds.AddCommand(NewCmdRemoveChangefeed(f, o))
	cmds.AddCommand(NewCmdCreateChangefeed(f, o))
	cmds.AddCommand(NewCmdUpdateChangefeed(f, o))
	cmds.AddCommand(NewCmdStatisticsChangefeed(f, o))
	cmds.AddCommand(NewCmdCyclicChangefeed(f, o))

	cmds.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmds.PersistentFlags().BoolVar(&o.NoConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")
	_ = cmds.MarkPersistentFlagRequired("changefeed-id")

	return cmds
}

func applyOwnerChangefeedQuery(etcdClient *kv.CDCEtcdClient,
	ctx context.Context, cid model.ChangeFeedID, credential *security.Credential,
) (string, error) {
	owner, err := capture.GetOwnerCapture(etcdClient, ctx)
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

func applyAdminChangefeed(etcdClient *kv.CDCEtcdClient, ctx context.Context, job model.AdminJob, credential *security.Credential) error {
	owner, err := capture.GetOwnerCapture(etcdClient, ctx)
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

func confirmLargeDataGap(f util.Factory, ctx context.Context, cmd *cobra.Command, commonOptions *commonOptions, startTs uint64) error {
	if commonOptions.NoConfirm {
		return nil
	}
	pdClient, err := f.PdClient()
	if err != nil {
		return err
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
