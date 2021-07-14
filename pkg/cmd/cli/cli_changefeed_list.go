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
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// changefeedCommonInfo holds some common used information of a changefeed.
type changefeedCommonInfo struct {
	ID      string              `json:"id"`
	Summary *cdc.ChangefeedResp `json:"summary"`
}

// listChangefeedOptions defines flags for the `cli changefeed list` command.
type listChangefeedOptions struct {
	changefeedListAll bool
}

// newListChangefeedOptions creates new options for the `cli changefeed list` command.
func newListChangefeedOptions() *listChangefeedOptions {
	return &listChangefeedOptions{}
}

// newCmdListChangefeed creates the `cli changefeed list` command.
func newCmdListChangefeed(f util.Factory) *cobra.Command {
	o := newListChangefeedOptions()

	command := &cobra.Command{
		Use:   "list",
		Short: "List all replication tasks (changefeeds) in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()

			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			_, raw, err := etcdClient.GetChangeFeeds(ctx)
			if err != nil {
				return err
			}

			changefeedIDs := make(map[string]struct{}, len(raw))
			for id := range raw {
				changefeedIDs[id] = struct{}{}
			}

			if o.changefeedListAll {
				statuses, err := etcdClient.GetAllChangeFeedStatus(ctx)
				if err != nil {
					return err
				}
				for cid := range statuses {
					changefeedIDs[cid] = struct{}{}
				}
			}

			cfs := make([]*changefeedCommonInfo, 0, len(changefeedIDs))
			for id := range changefeedIDs {
				cfci := &changefeedCommonInfo{ID: id}
				resp, err := applyOwnerChangefeedQuery(ctx, etcdClient, id, f.GetCredential())
				if err != nil {
					// if no capture is available, the query will fail, just add a warning here
					log.Warn("query changefeed info failed", zap.String("error", err.Error()))
				} else {
					info := &cdc.ChangefeedResp{}
					err = json.Unmarshal([]byte(resp), info)
					if err != nil {
						return err
					}
					cfci.Summary = info
				}
				cfs = append(cfs, cfci)
			}

			return util.JSONPrint(cmd, cfs)
		},
	}

	command.PersistentFlags().BoolVarP(&o.changefeedListAll, "all", "a", false, "List all replication tasks(including removed and finished)")
	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}
