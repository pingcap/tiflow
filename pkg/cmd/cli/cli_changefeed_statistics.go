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
	"time"

	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
)

// status specifies the current status of the changefeed.
type status struct {
	SinkGap        string `json:"sink_gap"`
	ReplicationGap string `json:"replication_gap"`
}

// statisticsChangefeedOptions defines flags for the `cli changefeed statistics` command.
type statisticsChangefeedOptions struct {
	apiClient apiv2client.APIV2Interface

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
	var err error
	o.apiClient, err = f.APIV2Client()
	if err != nil {
		return err
	}
	return nil
}

// run cli command with api client
func (o *statisticsChangefeedOptions) runCliWithAPIClient(ctx context.Context, cmd *cobra.Command, lastCount *uint64, lastTime *time.Time) error {
	now := time.Now()
	var count uint64

	changefeed, err := o.apiClient.Changefeeds().Get(ctx, o.changefeedID)
	if err != nil {
		return err
	}
	ts, err := o.apiClient.Tso().Query(ctx,
		&v2.UpstreamConfig{ID: changefeed.UpstreamID})
	if err != nil {
		return err
	}

	sinkGap := oracle.ExtractPhysical(changefeed.ResolvedTs) -
		oracle.ExtractPhysical(changefeed.CheckpointTs)
	replicationGap := ts.Timestamp - oracle.ExtractPhysical(changefeed.CheckpointTs)
	statistics := status{
		SinkGap:        fmt.Sprintf("%dms", sinkGap),
		ReplicationGap: fmt.Sprintf("%dms", replicationGap),
	}

	*lastCount = count
	*lastTime = now
	return util.JSONPrint(cmd, statistics)
}

// run the `cli changefeed statistics` command.
func (o *statisticsChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	tick := time.NewTicker(time.Duration(o.interval) * time.Second)
	var lastTime time.Time
	var lastCount uint64
	_ = o.runCliWithAPIClient(ctx, cmd, &lastCount, &lastTime)
	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				return err
			}
		case <-tick.C:
			_ = o.runCliWithAPIClient(ctx, cmd, &lastCount, &lastTime)
		}
	}
}

// newCmdStatisticsChangefeed creates the `cli changefeed statistics` command.
func newCmdStatisticsChangefeed(f factory.Factory) *cobra.Command {
	o := newStatisticsChangefeedOptions()

	command := &cobra.Command{
		Use:   "statistics",
		Short: "Periodically check and output the status of a replication task (changefeed)",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}

	o.addFlags(command)

	return command
}
