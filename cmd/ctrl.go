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
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3/concurrency"
)

var (
	changefeedID *string
	captureID    *string
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

// cfMeta holds changefeed info and changefeed status
type cfMeta struct {
	Info   *model.ChangeFeedInfo   `json:"info"`
	Status *model.ChangeFeedStatus `json:"status"`
}

type processorMeta struct {
	Status   *model.TaskStatus   `json:"status"`
	Position *model.TaskPosition `json:"position"`
}

func jsonPrint(cmd *cobra.Command, v interface{}) error {
	data, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	cmd.Printf("%s\n", data)
	return nil
}

func newListCaptureCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all captures in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, raw, err := cdcEtcdCli.GetCaptures(context.Background())
			if err != nil {
				return err
			}
			ownerID, err := cdcEtcdCli.GetOwnerID(context.Background(), kv.CaptureOwnerKey)
			if err != nil && errors.Cause(err) != concurrency.ErrElectionNoLeader {
				return err
			}
			captures := make([]*capture, 0, len(raw))
			for _, c := range raw {
				isOwner := c.ID == ownerID
				captures = append(captures, &capture{ID: c.ID, IsOwner: isOwner})
			}
			return jsonPrint(cmd, captures)
		},
	}
	return command
}

func newListChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all replication tasks (changefeeds) in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, raw, err := cdcEtcdCli.GetChangeFeeds(context.Background())
			if err != nil {
				return err
			}
			cfs := make([]*cf, 0, len(raw))
			for id := range raw {
				cfs = append(cfs, &cf{ID: id})
			}
			return jsonPrint(cmd, cfs)
		},
	}
	return command
}

func newListProcessorCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all processors in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, processors, err := cdcEtcdCli.GetAllProcessors(context.Background())
			if err != nil {
				return err
			}
			return jsonPrint(cmd, processors)
		},
	}
	return command
}

func newQueryChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replicaiton task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := cdcEtcdCli.GetChangeFeedInfo(context.Background(), *changefeedID)
			if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
				return err
			}
			status, err := cdcEtcdCli.GetChangeFeedStatus(context.Background(), *changefeedID)
			if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
				return err
			}
			meta := &cfMeta{Info: info, Status: status}
			return jsonPrint(cmd, meta)
		},
	}
	changefeedID = command.PersistentFlags().String("changefeed-id", "", "Replication task (changefeed) ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	return command
}

func newQueryProcessorCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a sub replication task (processor)",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, status, err := cdcEtcdCli.GetTaskStatus(context.Background(), *changefeedID, *captureID)
			if err != nil && errors.Cause(err) != model.ErrTaskStatusNotExists {
				return err
			}
			_, position, err := cdcEtcdCli.GetTaskPosition(context.Background(), *changefeedID, *captureID)
			if err != nil && errors.Cause(err) != model.ErrTaskPositionNotExists {
				return err
			}
			meta := &processorMeta{Status: status, Position: position}
			return jsonPrint(cmd, meta)
		},
	}
	changefeedID = command.PersistentFlags().String("changefeed-id", "", "Replication task (changefeed) ID")
	captureID = command.PersistentFlags().String("capture-id", "", "Capture ID")

	_ = command.MarkPersistentFlagRequired("changefeed-id")
	_ = command.MarkPersistentFlagRequired("capture-id")
	return command
}

func newQueryTsoCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Get tso from PD",
		RunE: func(cmd *cobra.Command, args []string) error {
			ts, logic, err := pdCli.GetTS(context.Background())
			if err != nil {
				return err
			}
			cmd.Println(oracle.ComposeTS(ts, logic))
			return nil
		},
	}
	command.SetOutput(os.Stdout)
	return command
}

func newDeleteMetaCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "delete",
		Short: "Delete all meta data in etcd, confirm that you know what this command will do and use it at your own risk",
		RunE: func(cmd *cobra.Command, args []string) error {
			err := cdcEtcdCli.ClearAllCDCInfo(context.Background())
			if err == nil {
				cmd.Println("already truncate all meta in etcd!")
			}
			return err
		},
	}
	return command
}
