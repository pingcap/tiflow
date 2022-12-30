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
	"io"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

const (
	// tsGapWarning specifies the OOM threshold.
	// 1 day in milliseconds
	tsGapWarning = 86400 * 1000
)

func readInput(cmd *cobra.Command) bool {
	var yOrN string
	_, err := fmt.Scan(&yOrN)
	if err != nil {
		cmd.Printf("Received invalid input: %s, abort the command.\n", err.Error())
		return false
	}
	if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
		return false
	}
	return true
}

// confirmLargeDataGap checks if a large data gap is used.
func confirmLargeDataGap(cmd *cobra.Command, currentPhysical int64, startTs uint64) error {
	tsGap := currentPhysical - oracle.ExtractPhysical(startTs)

	if tsGap > tsGapWarning {
		cmd.Printf("Replicate lag (%s) is larger than 1 days, "+
			"large data may cause OOM, confirm to continue at your own risk [Y/N]\n",
			time.Duration(tsGap)*time.Millisecond,
		)
		confirmed := readInput(cmd)
		if !confirmed {
			return errors.NewNoStackError("abort changefeed create or resume")
		}
	}

	return nil
}

// confirmIgnoreIneligibleTables confirm if user need to ignore ineligible tables.
func confirmIgnoreIneligibleTables(cmd *cobra.Command) error {
	cmd.Printf("Could you agree to ignore those tables, and continue to replicate [Y/N]\n")
	confirmed := readInput(cmd)
	if !confirmed {
		cmd.Printf("No changefeed is created because you don't want to ignore some tables.\n")
		return errors.NewNoStackError("abort changefeed create or resume")
	}

	return nil
}

// getTables returns ineligibleTables and eligibleTables by filter.
func getTables(cliPdAddr string, credential *security.Credential, cfg *config.ReplicaConfig, startTs uint64) (ineligibleTables, eligibleTables []model.TableName, err error) {
	kvStore, err := kv.CreateTiStore(cliPdAddr, credential)
	if err != nil {
		return nil, nil, err
	}

	return api.VerifyTables(cfg, kvStore, startTs)
}

// sendOwnerChangefeedQuery sends owner changefeed query request.
func sendOwnerChangefeedQuery(ctx context.Context, etcdClient *etcd.CDCEtcdClient,
	id model.ChangeFeedID, credential *security.Credential,
) (string, error) {
	owner, err := getOwnerCapture(ctx, etcdClient)
	if err != nil {
		return "", err
	}

	scheme := util.HTTP
	if credential.IsTLSEnabled() {
		scheme = util.HTTPS
	}

	url := fmt.Sprintf("%s://%s/capture/owner/changefeed/query", scheme, owner.AdvertiseAddr)
	httpClient, err := httputil.NewClient(credential)
	if err != nil {
		return "", err
	}

	resp, err := httpClient.PostForm(ctx, url, map[string][]string{
		api.OpVarChangefeedID: {id.ID},
	})
	if err != nil {
		return "", err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", errors.BadRequestf("query changefeed simplified status")
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", errors.BadRequestf("%s", string(body))
	}

	return string(body), nil
}

// sendOwnerAdminChangeQuery sends owner admin query request.
func sendOwnerAdminChangeQuery(ctx context.Context, etcdClient *etcd.CDCEtcdClient, job model.AdminJob, credential *security.Credential) error {
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

	resp, err := httpClient.PostForm(ctx, url, map[string][]string{
		api.OpVarAdminJob:           {fmt.Sprint(int(job.Type))},
		api.OpVarChangefeedID:       {job.CfID.ID},
		api.OpForceRemoveChangefeed: {forceRemoveOpt},
	})
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.BadRequestf("admin changefeed failed")
		}
		return errors.BadRequestf("%s", string(body))
	}

	return nil
}
