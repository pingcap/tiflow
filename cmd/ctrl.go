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
	"github.com/pingcap/ticdc/cdc/kv"
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
	// query subchangefeed replication status
	CtrlQuerySubCf = "query-sub-cf"
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
			info, err := kv.GetChangeFeedDetail(context.Background(), cli, ctrlCfID)
			if err != nil {
				return err
			}
			return jsonPrint(info)
		case CtrlQueryCfStatus:
			info, err := kv.GetChangeFeedInfo(context.Background(), cli, ctrlCfID)
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
			_, captures, err := kv.GetCaptures(context.Background(), cli)
			if err != nil {
				return err
			}
			return jsonPrint(captures)
		case CtrlQuerySubCf:
			_, info, err := kv.GetSubChangeFeedInfo(context.Background(), cli, ctrlCfID, ctrlCaptureID)
			if err != nil {
				return err
			}
			return jsonPrint(info)
		default:
			fmt.Printf("unknown controller command: %s\n", ctrlCommand)
		}
		return nil
	},
}
