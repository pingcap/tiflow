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
	"fmt"
	"time"

	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/factory"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
)

// status specifies the current status of the changefeed.
type status struct {
	OPS            uint64 `json:"ops"`
	Count          uint64 `json:"count"`
	SinkGap        string `json:"sink_gap"`
	ReplicationGap string `json:"replication_gap"`
}

// statisticsChangefeedOptions defines flags for the `cli changefeed statistics` command.
type statisticsChangefeedOptions struct {
	etcdClient *kv.CDCEtcdClient
	pdClient   pd.Client

	changefeedID string
	interval     uint
}

// newStatisticsChangefeedOptions creates new options for the `cli changefeed statistics` command.
func newStatisticsChangefeedOptions() *statisticsChangefeedOptions {
	return &statisticsChangefeedOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *statisticsChangefeedOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().UintVarP(&o.interval, "interval", "I", 10, "Interval for outputing the latest statistics")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *statisticsChangefeedOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}

	o.pdClient = pdClient

	return nil
}

// run the `cli changefeed statistics` command.
func (o *statisticsChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := context.GetDefaultContext()

	tick := time.NewTicker(time.Duration(o.interval) * time.Second)
	lastTime := time.Now()

	var lastCount uint64

	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				return err
			}
		case <-tick.C:
			now := time.Now()

			changefeedStatus, _, err := o.etcdClient.GetChangeFeedStatus(ctx, o.changefeedID)
			if err != nil {
				return err
			}

			taskPositions, err := o.etcdClient.GetAllTaskPositions(ctx, o.changefeedID)
			if err != nil {
				return err
			}

			var count uint64
			for _, pinfo := range taskPositions {
				count += pinfo.Count
			}

			ts, _, err := o.pdClient.GetTS(ctx)
			if err != nil {
				return err
			}

			sinkGap := oracle.ExtractPhysical(changefeedStatus.ResolvedTs) - oracle.ExtractPhysical(changefeedStatus.CheckpointTs)
			replicationGap := ts - oracle.ExtractPhysical(changefeedStatus.CheckpointTs)

			statistics := status{
				OPS:            (count - lastCount) / uint64(now.Unix()-lastTime.Unix()),
				SinkGap:        fmt.Sprintf("%dms", sinkGap),
				ReplicationGap: fmt.Sprintf("%dms", replicationGap),
				Count:          count,
			}

			_ = util.JSONPrint(cmd, &statistics)

			lastCount = count
			lastTime = now
		}
	}
}

// newCmdStatisticsChangefeed creates the `cli changefeed statistics` command.
func newCmdStatisticsChangefeed(f factory.Factory) *cobra.Command {
	o := newStatisticsChangefeedOptions()

	command := &cobra.Command{
		Use:   "statistics",
		Short: "Periodically check and output the status of a replication task (changefeed)",
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
