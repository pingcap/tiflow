// Copyright 2019 PingCAP, Inc.
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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/roles"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

// command type
const (
	// query changefeed info, returns a json marshalled ChangeFeedDetail
	CtrlQueryCfInfo = "query-cf-info"
	// query changefeed replication status
	CtrlQueryCfStatus = "query-cf-status"
	// query changefeed list
	CtrlQueryCfs = "query-cf-list"
	// query capture list
	CtrlQueryCaptures = "query-capture-list"
	// query processor replication status
	CtrlQuerySubCf = "query-sub-cf"
	// clear all key-values created by CDC
	CtrlClearAll = "clear-all"
	// get tso from pd
	CtrlGetTso = "get-tso"
)

func init() {
	rootCmd.AddCommand(ctrlCmd)

	ctrlCmd.Flags().StringVar(&ctrlPdAddr, "pd-addr", "localhost:2379", "address of PD")
	ctrlCmd.Flags().StringVar(&ctrlCfID, "changefeed-id", "", "changefeed ID")
	ctrlCmd.Flags().StringVar(&ctrlCaptureID, "capture-id", "", "capture ID")
	ctrlCmd.Flags().StringVar(&ctrlCommand, "cmd", CtrlQueryCaptures, "controller command type")
}

var (
	ctrlPdAddr    string
	ctrlCfID      string
	ctrlCaptureID string
	ctrlCommand   string
)

// cf holds changefeed id, which is used for output only
type cf struct {
	ID string `json:"id"`
}

// capture holds capture information
type capture struct {
	ID      string `json:"id"`
	IsOwner bool   `json:"is-owner"`
}

func jsonPrint(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", data)
	return nil
}

var ctrlCmd = &cobra.Command{
	Use:   "ctrl",
	Short: "cdc controller",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{ctrlPdAddr},
			DialTimeout: 5 * time.Second,
			DialOptions: []grpc.DialOption{
				grpc.WithBackoffMaxDelay(time.Second * 3),
			},
		})
		if err != nil {
			return err
		}
		switch ctrlCommand {
		case CtrlQueryCfInfo:
			info, err := kv.GetChangeFeedInfo(context.Background(), cli, ctrlCfID)
			if err != nil {
				return err
			}
			return jsonPrint(info)
		case CtrlQueryCfStatus:
			info, err := kv.GetChangeFeedStatus(context.Background(), cli, ctrlCfID)
			if err != nil {
				return err
			}
			return jsonPrint(info)
		case CtrlQueryCfs:
			_, raw, err := kv.GetChangeFeeds(context.Background(), cli)
			if err != nil {
				return err
			}
			cfs := make([]*cf, 0, len(raw))
			for id := range raw {
				cfs = append(cfs, &cf{ID: id})
			}
			return jsonPrint(cfs)
		case CtrlQueryCaptures:
			_, raw, err := kv.GetCaptures(context.Background(), cli)
			if err != nil {
				return err
			}
			ownerID, err := roles.GetOwnerID(context.Background(), cli, kv.CaptureOwnerKey)
			if err != nil {
				return err
			}
			captures := make([]*capture, 0, len(raw))
			for _, c := range raw {
				isOwner := c.ID == ownerID
				captures = append(captures, &capture{ID: c.ID, IsOwner: isOwner})
			}
			return jsonPrint(captures)
		case CtrlQuerySubCf:
			_, info, err := kv.GetTaskStatus(context.Background(), cli, ctrlCfID, ctrlCaptureID)
			if err != nil && err != concurrency.ErrElectionNoLeader {
				return err
			}
			return jsonPrint(info)
		case CtrlClearAll:
			return kv.ClearAllCDCInfo(context.Background(), cli)
		case CtrlGetTso:
			pdCli, err := pd.NewClient([]string{ctrlPdAddr}, pd.SecurityOption{})
			if err != nil {
				return err
			}
			ts, logic, err := pdCli.GetTS(context.Background())
			if err != nil {
				return err
			}
			fmt.Println(oracle.ComposeTS(ts, logic))
		default:
			fmt.Printf("unknown controller command: %s\n", ctrlCommand)
		}
		return nil
	},
}
