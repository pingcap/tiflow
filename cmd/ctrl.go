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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	"go.etcd.io/etcd/clientv3/concurrency"
)

var (
	changefeedID string
	captureID    string
	interval     uint
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
	Info       *model.ChangeFeedInfo   `json:"info"`
	Status     *model.ChangeFeedStatus `json:"status"`
	Count      uint64                  `json:"count"`
	TaskStatus []captureTaskStatus     `json:"task-status"`
}

type captureTaskStatus struct {
	CaptureID  string            `json:"capture-id"`
	TaskStatus *model.TaskStatus `json:"status"`
}

type profileStatus struct {
	OPS            uint64 `json:"ops"`
	Count          uint64 `json:"count"`
	SinkGap        string `json:"sink_gap"`
	ReplicationGap string `json:"replication_gap"`
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

func newQueryChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replicaiton task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := cdcEtcdCli.GetChangeFeedInfo(context.Background(), changefeedID)
			if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
				return err
			}
			status, _, err := cdcEtcdCli.GetChangeFeedStatus(context.Background(), changefeedID)
			if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
				return err
			}
			taskPositions, err := cdcEtcdCli.GetAllTaskPositions(context.Background(), changefeedID)
			if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
				return err
			}
			var count uint64
			for _, pinfo := range taskPositions {
				count += pinfo.Count
			}
			processorInfos, err := cdcEtcdCli.GetAllTaskStatus(context.Background(), changefeedID)
			if err != nil {
				return err
			}
			taskStatus := make([]captureTaskStatus, 0, len(processorInfos))
			for captureID, status := range processorInfos {
				taskStatus = append(taskStatus, captureTaskStatus{CaptureID: captureID, TaskStatus: status})
			}
			meta := &cfMeta{Info: info, Status: status, Count: count, TaskStatus: taskStatus}
			return jsonPrint(cmd, meta)
		},
	}
	command.PersistentFlags().StringVar(&changefeedID, "changefeed-id", "", "Replication task (changefeed) ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	return command
}

func newStatisticsChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "statistics",
		Short: "Periodically check and output the status of a replicaiton task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			sc := make(chan os.Signal, 1)
			signal.Notify(sc,
				syscall.SIGHUP,
				syscall.SIGINT,
				syscall.SIGTERM,
				syscall.SIGQUIT)

			tick := time.NewTicker(time.Duration(interval) * time.Second)
			lastTime := time.Now()
			var lastCount uint64
			for {
				select {
				case sig := <-sc:
					switch sig {
					case syscall.SIGTERM:
						os.Exit(0)
					default:
						os.Exit(1)
					}
				case <-tick.C:
					now := time.Now()
					status, _, err := cdcEtcdCli.GetChangeFeedStatus(context.Background(), changefeedID)
					if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
						return err
					}
					taskPositions, err := cdcEtcdCli.GetAllTaskPositions(context.Background(), changefeedID)
					if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
						return err
					}
					var count uint64
					for _, pinfo := range taskPositions {
						count += pinfo.Count
					}
					ts, _, err := pdCli.GetTS(context.Background())
					if err != nil {
						return err
					}
					sinkGap := oracle.ExtractPhysical(status.ResolvedTs) - oracle.ExtractPhysical(status.CheckpointTs)
					replicationGap := ts - oracle.ExtractPhysical(status.CheckpointTs)
					statistics := profileStatus{
						OPS:            (count - lastCount) / uint64(now.Unix()-lastTime.Unix()),
						SinkGap:        fmt.Sprintf("%dms", sinkGap),
						ReplicationGap: fmt.Sprintf("%dms", replicationGap),
						Count:          count,
					}
					jsonPrint(cmd, &statistics)
					lastCount = count
					lastTime = now
				}
			}
		},
	}
	command.PersistentFlags().StringVar(&changefeedID, "changefeed-id", "", "Replication task (changefeed) ID")
	command.PersistentFlags().UintVar(&interval, "interval", 10, "Interval for outputing the latest statistics")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	return command
}

func newListProcessorCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all processors in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			info, err := cdcEtcdCli.GetProcessors(context.Background())
			if err != nil {
				return err
			}
			return jsonPrint(cmd, info)
		},
	}
	return command
}

func newQueryProcessorCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a sub replication task (processor)",
		RunE: func(cmd *cobra.Command, args []string) error {
			_, status, err := cdcEtcdCli.GetTaskStatus(context.Background(), changefeedID, captureID)
			if err != nil && errors.Cause(err) != model.ErrTaskStatusNotExists {
				return err
			}
			_, position, err := cdcEtcdCli.GetTaskPosition(context.Background(), changefeedID, captureID)
			if err != nil && errors.Cause(err) != model.ErrTaskPositionNotExists {
				return err
			}
			meta := &processorMeta{Status: status, Position: position}
			return jsonPrint(cmd, meta)
		},
	}
	command.PersistentFlags().StringVar(&changefeedID, "changefeed-id", "", "Replication task (changefeed) ID")
	command.PersistentFlags().StringVar(&captureID, "capture-id", "", "Capture ID")
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
