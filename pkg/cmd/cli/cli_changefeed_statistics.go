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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	apiv1client "github.com/pingcap/tiflow/pkg/api/v1"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
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
	etcdClient *etcd.CDCEtcdClient
	pdClient   pd.Client
	apiClient  apiv1client.APIV1Interface

	changefeedID     string
	interval         uint
	runWithAPIClient bool
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
	ctx := cmdcontext.GetDefaultContext()

	owner, err := getOwnerCapture(ctx, o.etcdClient)
	if err != nil {
		return err
	}

	o.apiClient, err = apiv1client.NewAPIClient(owner.AdvertiseAddr, f.GetCredential())
	if err != nil {
		return err
	}

	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}
	o.pdClient = pdClient

	_, captureInfos, err := o.etcdClient.GetCaptures(ctx)
	if err != nil {
		return err
	}
	cdcClusterVer, err := version.GetTiCDCClusterVersion(model.ListVersionsFromCaptureInfos(captureInfos))
	if err != nil {
		return errors.Trace(err)
	}

	o.runWithAPIClient = true
	if !cdcClusterVer.ShouldRunCliWithAPIClientByDefault() {
		o.runWithAPIClient = false
		log.Warn("The TiCDC cluster is built from an older version, run cli with etcd client by default.",
			zap.String("version", cdcClusterVer.String()))
	}

	return nil
}

// run cli command with etcd client
func (o *statisticsChangefeedOptions) runCliWithEtcdClient(ctx context.Context, cmd *cobra.Command, lastCount *uint64, lastTime *time.Time) error {
	now := time.Now()

	changefeedStatus, _, err := o.etcdClient.GetChangeFeedStatus(ctx,
		model.DefaultChangeFeedID(o.changefeedID))
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
		OPS:            (count - (*lastCount)) / uint64(now.Unix()-lastTime.Unix()),
		SinkGap:        fmt.Sprintf("%dms", sinkGap),
		ReplicationGap: fmt.Sprintf("%dms", replicationGap),
		Count:          count,
	}

	*lastCount = count
	*lastTime = now
	return util.JSONPrint(cmd, statistics)
}

// run cli command with api client
func (o *statisticsChangefeedOptions) runCliWithAPIClient(ctx context.Context, cmd *cobra.Command, lastCount *uint64, lastTime *time.Time) error {
	now := time.Now()
	var count uint64
	captures, err := o.apiClient.Captures().List(ctx)
	if err != nil {
		return err
	}

	for _, capture := range *captures {
		processor, err := o.apiClient.Processors().Get(ctx, o.changefeedID, capture.ID)
		if err != nil {
			return err
		}
		count += processor.Count
	}

	ts, _, err := o.pdClient.GetTS(ctx)
	if err != nil {
		return err
	}
	changefeed, err := o.apiClient.Changefeeds().Get(ctx, o.changefeedID)
	if err != nil {
		return err
	}

	sinkGap := oracle.ExtractPhysical(changefeed.ResolvedTs) - oracle.ExtractPhysical(changefeed.CheckpointTSO)
	replicationGap := ts - oracle.ExtractPhysical(changefeed.CheckpointTSO)
	statistics := status{
		OPS:            (count - (*lastCount)) / uint64(now.Unix()-lastTime.Unix()),
		SinkGap:        fmt.Sprintf("%dms", sinkGap),
		ReplicationGap: fmt.Sprintf("%dms", replicationGap),
		Count:          count,
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

	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != nil {
				return err
			}
		case <-tick.C:
			if o.runWithAPIClient {
				_ = o.runCliWithAPIClient(ctx, cmd, &lastCount, &lastTime)
			} else {
				_ = o.runCliWithEtcdClient(ctx, cmd, &lastCount, &lastTime)
			}
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
