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
	"fmt"
	"time"

	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

type profileStatus struct {
	OPS            uint64 `json:"ops"`
	Count          uint64 `json:"count"`
	SinkGap        string `json:"sink_gap"`
	ReplicationGap string `json:"replication_gap"`
}

// statisticsChangefeedOptions defines flags for the `cli changefeed statistics` command.
type statisticsChangefeedOptions struct {
	interval uint
}

// newStatisticsChangefeedOptions creates new options for the `cli changefeed statistics` command.
func newStatisticsChangefeedOptions() *statisticsChangefeedOptions {
	return &statisticsChangefeedOptions{}
}

// newCmdStatisticsChangefeed creates the `cli changefeed statistics` command.
func newCmdStatisticsChangefeed(f util.Factory, commonOptions *commonOptions) *cobra.Command {
	o := newStatisticsChangefeedOptions()

	command := &cobra.Command{
		Use:   "statistics",
		Short: "Periodically check and output the status of a replicaiton task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()
			tick := time.NewTicker(time.Duration(o.interval) * time.Second)
			lastTime := time.Now()
			var lastCount uint64
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}
			pdClient, err := f.PdClient()
			if err != nil {
				return err
			}
			for {
				select {
				case <-ctx.Done():
					if err := ctx.Err(); err != nil {
						return err
					}
				case <-tick.C:
					now := time.Now()
					status, _, err := etcdClient.GetChangeFeedStatus(ctx, commonOptions.changefeedID)
					if err != nil {
						return err
					}
					taskPositions, err := etcdClient.GetAllTaskPositions(ctx, commonOptions.changefeedID)
					if err != nil {
						return err
					}
					var count uint64
					for _, pinfo := range taskPositions {
						count += pinfo.Count
					}
					ts, _, err := pdClient.GetTS(ctx)
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
					_ = util.JsonPrint(cmd, &statistics)
					lastCount = count
					lastTime = now
				}
			}
		},
	}

	command.PersistentFlags().UintVarP(&o.interval, "interval", "I", 10, "Interval for outputing the latest statistics")
	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}
