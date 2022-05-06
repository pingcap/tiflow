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
	"encoding/json"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/api"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// changefeedCommonInfo holds some common used information of a changefeed.
type changefeedCommonInfo struct {
	ID      string              `json:"id"`
	Summary *api.ChangefeedResp `json:"summary"`
}

// listChangefeedOptions defines flags for the `cli changefeed list` command.
type listChangefeedOptions struct {
	etcdClient *etcd.CDCEtcdClient

	credential *security.Credential

	listAll bool
}

// newListChangefeedOptions creates new options for the `cli changefeed list` command.
func newListChangefeedOptions() *listChangefeedOptions {
	return &listChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *listChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVarP(&o.listAll, "all", "a", false, "List all replication tasks(including removed and finished)")
}

// complete adapts from the command line args to the data and client required.
func (o *listChangefeedOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	o.credential = f.GetCredential()

	return nil
}

// run the `cli changefeed list` command.
func (o *listChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	_, raw, err := o.etcdClient.GetChangeFeeds(ctx)
	if err != nil {
		return err
	}

	changefeedIDs := make(map[model.ChangeFeedID]struct{}, len(raw))
	for id := range raw {
		changefeedIDs[id] = struct{}{}
	}

	if o.listAll {
		statuses, err := o.etcdClient.GetAllChangeFeedStatus(ctx)
		if err != nil {
			return err
		}
		for cid := range statuses {
			changefeedIDs[cid] = struct{}{}
		}
	}

	cfs := make([]*changefeedCommonInfo, 0, len(changefeedIDs))

	for id := range changefeedIDs {
		cfci := &changefeedCommonInfo{ID: id.ID}

		resp, err := sendOwnerChangefeedQuery(ctx, o.etcdClient, id, o.credential)
		if err != nil {
			// if no capture is available, the query will fail, just add a warning here
			log.Warn("query changefeed info failed", zap.String("error", err.Error()))
		} else {
			info := &api.ChangefeedResp{}
			err = json.Unmarshal([]byte(resp), info)
			if err != nil {
				return err
			}

			cfci.Summary = info
		}
		cfs = append(cfs, cfci)
	}

	return util.JSONPrint(cmd, cfs)
}

// newCmdListChangefeed creates the `cli changefeed list` command.
func newCmdListChangefeed(f factory.Factory) *cobra.Command {
	o := newListChangefeedOptions()

	command := &cobra.Command{
		Use:   "list",
		Short: "List all replication tasks (changefeeds) in TiCDC cluster",
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
