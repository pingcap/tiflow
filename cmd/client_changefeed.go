package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	cdcfilter "github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
)

func newChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
	}
	command.AddCommand(
		newListChangefeedCommand(),
		newQueryChangefeedCommand(),
		newCreateChangefeedCommand(),
		newStatisticsChangefeedCommand(),
	)
	// Add pause, resume, remove changefeed
	for _, cmd := range newAdminChangefeedCommand() {
		command.AddCommand(cmd)
	}
	return command
}

func newAdminChangefeedCommand() []*cobra.Command {
	cmds := []*cobra.Command{
		{
			Use:   "pause",
			Short: "Pause a replicaiton task (changefeed)",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := contextTimeout()
				defer cancel()
				job := model.AdminJob{
					CfID: changefeedID,
					Type: model.AdminStop,
				}
				return applyAdminChangefeed(ctx, job)
			},
		},
		{
			Use:   "resume",
			Short: "Resume a paused replicaiton task (changefeed)",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := contextTimeout()
				defer cancel()
				job := model.AdminJob{
					CfID: changefeedID,
					Type: model.AdminResume,
				}
				return applyAdminChangefeed(ctx, job)
			},
		},
		{
			Use:   "remove",
			Short: "Remove a replicaiton task (changefeed)",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx, cancel := contextTimeout()
				defer cancel()
				job := model.AdminJob{
					CfID: changefeedID,
					Type: model.AdminRemove,
				}
				return applyAdminChangefeed(ctx, job)
			},
		},
	}

	for _, cmd := range cmds {
		cmd.PersistentFlags().StringVar(&changefeedID, "changefeed-id", "", "Replication task (changefeed) ID")
		_ = cmd.MarkPersistentFlagRequired("changefeed-id")
	}
	return cmds
}

func newListChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all replication tasks (changefeeds) in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := contextTimeout()
			defer cancel()
			_, raw, err := cdcEtcdCli.GetChangeFeeds(ctx)
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
			ctx, cancel := contextTimeout()
			defer cancel()
			info, err := cdcEtcdCli.GetChangeFeedInfo(ctx, changefeedID)
			if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
				return err
			}
			status, _, err := cdcEtcdCli.GetChangeFeedStatus(ctx, changefeedID)
			if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
				return err
			}
			taskPositions, err := cdcEtcdCli.GetAllTaskPositions(ctx, changefeedID)
			if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
				return err
			}
			var count uint64
			for _, pinfo := range taskPositions {
				count += pinfo.Count
			}
			processorInfos, err := cdcEtcdCli.GetAllTaskStatus(ctx, changefeedID)
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

func newCreateChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "create",
		Short: "Create a new replication task (changefeed)",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := contextTimeout()
			defer cancel()
			id := uuid.New().String()
			if startTs == 0 {
				ts, logical, err := pdCli.GetTS(ctx)
				if err != nil {
					return err
				}
				startTs = oracle.ComposeTS(ts, logical)
			}
			if err := verifyStartTs(ctx, startTs, cdcEtcdCli); err != nil {
				return err
			}

			cfg := new(cdcfilter.ReplicaConfig)
			if len(configFile) > 0 {
				if err := strictDecodeFile(configFile, "cdc", cfg); err != nil {
					return err
				}
			}
			if cyclicReplicaID != 0 && len(cyclicFilterReplicaIDs) != 0 {
				filter := make([]uint64, 0, len(cyclicFilterReplicaIDs))
				for _, id := range cyclicFilterReplicaIDs {
					filter = append(filter, uint64(id))
				}
				cfg.Cyclic = &cdcfilter.ReplicationConfig{
					Enable:          true,
					ReplicaID:       cyclicReplicaID,
					FilterReplicaID: filter,
					SyncDDL:         cyclicSyncDDL,
					// TODO(neil) enable ID bucket.
				}
			}
			info := &model.ChangeFeedInfo{
				SinkURI:    sinkURI,
				Opts:       make(map[string]string),
				CreateTime: time.Now(),
				StartTs:    startTs,
				TargetTs:   targetTs,
				Config:     cfg,
				Engine:     model.SortEngine(sortEngine),
				SortDir:    sortDir,
			}

			ineligibleTables, err := verifyTables(ctx, cfg, startTs)
			if err != nil {
				return err
			}
			if len(ineligibleTables) != 0 {
				cmd.Printf("[WARN] some tables are not eligible to replicate, %#v\n", ineligibleTables)
				if !noConfirm {
					cmd.Printf("Could you agree to ignore those tables, and continue to replicate [Y/N]\n")
					var yOrN string
					_, err := fmt.Scan(&yOrN)
					if err != nil {
						return err
					}
					if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
						cmd.Printf("No changefeed is created because you don't want to ignore some tables.\n")
						return nil
					}
				}
			}

			for _, opt := range opts {
				s := strings.SplitN(opt, "=", 2)
				if len(s) <= 0 {
					cmd.Printf("omit opt: %s", opt)
					continue
				}

				var key string
				var value string

				key = s[0]
				if len(s) > 1 {
					value = s[1]
				}
				info.Opts[key] = value
			}

			d, err := info.Marshal()
			if err != nil {
				return err
			}
			err = verifySink(ctx, info.SinkURI, info.Config, info.Opts)
			if err != nil {
				return err
			}
			err = cdcEtcdCli.SaveChangeFeedInfo(ctx, info, id)
			if err != nil {
				return err
			}
			cmd.Printf("Create changefeed successfully!\nID: %s\nInfo: %s\n", id, d)
			return nil
		},
	}
	command.PersistentFlags().Uint64Var(&startTs, "start-ts", 0, "Start ts of changefeed")
	command.PersistentFlags().Uint64Var(&targetTs, "target-ts", 0, "Target ts of changefeed")
	command.PersistentFlags().StringVar(&sinkURI, "sink-uri", "mysql://root:123456@127.0.0.1:3306/", "sink uri")
	command.PersistentFlags().StringVar(&configFile, "config", "", "Path of the configuration file")
	command.PersistentFlags().StringSliceVar(&opts, "opts", nil, "Extra options, in the `key=value` format")
	command.PersistentFlags().BoolVar(&noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")
	command.PersistentFlags().StringVar(&sortEngine, "sort-engine", "memory", "sort engine used for data sort")
	command.PersistentFlags().StringVar(&sortDir, "sort-dir", ".", "directory used for file sort")
	command.PersistentFlags().Uint64Var(&cyclicReplicaID, "cyclic-replica-id", 0, "(Expremental) Cyclic replication replica ID of changefeed")
	command.PersistentFlags().UintSliceVar(&cyclicFilterReplicaIDs, "cyclic-filter-replica-ids", []uint{}, "(Expremental) Cyclic replication filter replica ID of changefeed")
	command.PersistentFlags().BoolVar(&cyclicSyncDDL, "cyclic-sync-ddl", true, "(Expremental) Cyclic replication sync DDL of changefeed")

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

			ctx, cancel := context.WithCancel(context.TODO())
			defer cancel()
			tick := time.NewTicker(time.Duration(interval) * time.Second)
			lastTime := time.Now()
			var lastCount uint64
			for {
				select {
				case sig := <-sc:
					switch sig {
					case syscall.SIGTERM:
						cancel()
						os.Exit(0)
					default:
						cancel()
						os.Exit(1)
					}
				case <-tick.C:
					now := time.Now()
					status, _, err := cdcEtcdCli.GetChangeFeedStatus(ctx, changefeedID)
					if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
						return err
					}
					taskPositions, err := cdcEtcdCli.GetAllTaskPositions(ctx, changefeedID)
					if err != nil && errors.Cause(err) != model.ErrChangeFeedNotExists {
						return err
					}
					var count uint64
					for _, pinfo := range taskPositions {
						count += pinfo.Count
					}
					ts, _, err := pdCli.GetTS(ctx)
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
