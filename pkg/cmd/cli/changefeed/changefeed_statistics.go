package changefeed

import (
	"fmt"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	"time"
)

type profileStatus struct {
	OPS            uint64 `json:"ops"`
	Count          uint64 `json:"count"`
	SinkGap        string `json:"sink_gap"`
	ReplicationGap string `json:"replication_gap"`
}

type StatisticsChangefeedOptions struct {
	interval uint
}

func NewStatisticsChangefeedOptions() *StatisticsChangefeedOptions {
	return &StatisticsChangefeedOptions{}
}

func NewCmdStatisticsChangefeed(f util.Factory, commonOptions *commonOptions) *cobra.Command {
	o := NewStatisticsChangefeedOptions()

	command := &cobra.Command{
		Use:   "statistics",
		Short: "Periodically check and output the status of a replicaiton task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
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
					etcdClient, err := f.EtcdClient()
					if err != nil {
						return err
					}
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
					pdClient, err := f.PdClient()
					if err != nil {
						return err
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

	return command
}
