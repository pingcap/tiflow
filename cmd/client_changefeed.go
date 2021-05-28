// Copyright 2020 PingCAP, Inc.
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
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/r3labs/diff"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

const (
	// Use the empty string as the default to let the server local setting override the changefeed setting.
	// TODO remove this when we change the changefeed `sort-dir` to no-op, which it currently is NOT.
	defaultSortDir = ""
)

var forceEnableOldValueProtocols = []string{
	"canal",
	"maxwell",
}

func newChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
	}
	command.AddCommand(
		newListChangefeedCommand(),
		newQueryChangefeedCommand(),
		newCreateChangefeedCommand(),
		newUpdateChangefeedCommand(),
		newStatisticsChangefeedCommand(),
		newCreateChangefeedCyclicCommand(),
	)
	// Add pause, resume, remove changefeed
	for _, cmd := range newAdminChangefeedCommand() {
		command.AddCommand(cmd)
	}
	return command
}

func resumeChangefeedCheck(ctx context.Context, cmd *cobra.Command) error {
	resp, err := applyOwnerChangefeedQuery(ctx, changefeedID, getCredential())
	if err != nil {
		return err
	}
	info := &cdc.ChangefeedResp{}
	err = json.Unmarshal([]byte(resp), info)
	if err != nil {
		return err
	}
	return confirmLargeDataGap(ctx, cmd, info.TSO)
}

func newAdminChangefeedCommand() []*cobra.Command {
	cmds := []*cobra.Command{
		{
			Use:   "pause",
			Short: "Pause a replicaiton task (changefeed)",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := defaultContext
				job := model.AdminJob{
					CfID: changefeedID,
					Type: model.AdminStop,
				}
				return applyAdminChangefeed(ctx, job, getCredential())
			},
		},
		{
			Use:   "resume",
			Short: "Resume a paused replicaiton task (changefeed)",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := defaultContext
				job := model.AdminJob{
					CfID: changefeedID,
					Type: model.AdminResume,
				}
				if err := resumeChangefeedCheck(ctx, cmd); err != nil {
					return err
				}
				return applyAdminChangefeed(ctx, job, getCredential())
			},
		},
		{
			Use:   "remove",
			Short: "Remove a replicaiton task (changefeed)",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := defaultContext
				job := model.AdminJob{
					CfID: changefeedID,
					Type: model.AdminRemove,
					Opts: &model.AdminJobOption{
						ForceRemove: optForceRemove,
					},
				}
				return applyAdminChangefeed(ctx, job, getCredential())
			},
		},
	}

	for _, cmd := range cmds {
		cmd.PersistentFlags().StringVarP(&changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
		_ = cmd.MarkPersistentFlagRequired("changefeed-id")
		if cmd.Use == "remove" {
			cmd.PersistentFlags().BoolVarP(&optForceRemove, "force", "f", false, "remove all information of the changefeed")
		}
		if cmd.Use == "resume" {
			cmd.PersistentFlags().BoolVar(&noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")
		}
	}
	return cmds
}

func newListChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all replication tasks (changefeeds) in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			_, raw, err := cdcEtcdCli.GetChangeFeeds(ctx)
			if err != nil {
				return err
			}
			changefeedIDs := make(map[string]struct{}, len(raw))
			for id := range raw {
				changefeedIDs[id] = struct{}{}
			}
			if changefeedListAll {
				statuses, err := cdcEtcdCli.GetAllChangeFeedStatus(ctx)
				if err != nil {
					return err
				}
				for cid := range statuses {
					changefeedIDs[cid] = struct{}{}
				}
			}
			cfs := make([]*changefeedCommonInfo, 0, len(changefeedIDs))
			for id := range changefeedIDs {
				cfci := &changefeedCommonInfo{ID: id}
				resp, err := applyOwnerChangefeedQuery(ctx, id, getCredential())
				if err != nil {
					// if no capture is available, the query will fail, just add a warning here
					log.Warn("query changefeed info failed", zap.String("error", err.Error()))
				} else {
					info := &cdc.ChangefeedResp{}
					err = json.Unmarshal([]byte(resp), info)
					if err != nil {
						return err
					}
					cfci.Summary = info
				}
				cfs = append(cfs, cfci)
			}
			return jsonPrint(cmd, cfs)
		},
	}
	command.PersistentFlags().BoolVarP(&changefeedListAll, "all", "a", false, "List all replication tasks(including removed and finished)")
	return command
}

func newQueryChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replicaiton task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext

			if simplified {
				resp, err := applyOwnerChangefeedQuery(ctx, changefeedID, getCredential())
				if err != nil {
					return err
				}
				cmd.Println(resp)
				return nil
			}

			info, err := cdcEtcdCli.GetChangeFeedInfo(ctx, changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}
			status, _, err := cdcEtcdCli.GetChangeFeedStatus(ctx, changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
				return err
			}
			if err != nil && cerror.ErrChangeFeedNotExists.Equal(err) {
				log.Error("This changefeed does not exist", zap.String("changefeed", changefeedID))
				return err
			}
			taskPositions, err := cdcEtcdCli.GetAllTaskPositions(ctx, changefeedID)
			if err != nil && cerror.ErrChangeFeedNotExists.NotEqual(err) {
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
			if info == nil {
				log.Warn("This changefeed has been deleted, the residual meta data will be completely deleted within 24 hours.", zap.String("changgefeed", changefeedID))
			}
			return jsonPrint(cmd, meta)
		},
	}
	command.PersistentFlags().BoolVarP(&simplified, "simple", "s", false, "Output simplified replication status")
	command.PersistentFlags().StringVarP(&changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	return command
}

func verifyChangefeedParamers(ctx context.Context, cmd *cobra.Command, isCreate bool, credential *security.Credential, captureInfos []*model.CaptureInfo) (*model.ChangeFeedInfo, error) {
	if isCreate {
		if sinkURI == "" {
			return nil, errors.New("Creating chengfeed without a sink-uri")
		}
		if startTs == 0 {
			ts, logical, err := pdCli.GetTS(ctx)
			if err != nil {
				return nil, err
			}
			startTs = oracle.ComposeTS(ts, logical)
		}
		if err := verifyStartTs(ctx, startTs); err != nil {
			return nil, err
		}
		if err := confirmLargeDataGap(ctx, cmd, startTs); err != nil {
			return nil, err
		}
		if err := verifyTargetTs(ctx, startTs, targetTs); err != nil {
			return nil, err
		}
	}
	cdcClusterVer, err := version.GetTiCDCClusterVersion(captureInfos)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cfg := config.GetDefaultReplicaConfig()

	sortEngineFlag := cmd.Flag("sort-engine")
	if cdcClusterVer == version.TiCDCClusterVersion4_0 {
		cfg.EnableOldValue = false
		if !sortEngineFlag.Changed {
			sortEngine = model.SortInMemory
		}
		log.Warn("The TiCDC cluster is built from 4.0-release branch, the old-value and unified-sorter are disabled by default.")
	}
	if len(configFile) > 0 {
		if err := strictDecodeFile(configFile, "TiCDC changefeed", cfg); err != nil {
			return nil, err
		}
	}
	if disableGCSafePointCheck {
		cfg.CheckGCSafePoint = false
	}
	if cyclicReplicaID != 0 || len(cyclicFilterReplicaIDs) != 0 {
		if !(cyclicReplicaID != 0 && len(cyclicFilterReplicaIDs) != 0) {
			return nil, errors.New("invaild cyclic config, please make sure using " +
				"nonzero replica ID and specify filter replica IDs")
		}
		filter := make([]uint64, 0, len(cyclicFilterReplicaIDs))
		for _, id := range cyclicFilterReplicaIDs {
			filter = append(filter, uint64(id))
		}
		cfg.Cyclic = &config.CyclicConfig{
			Enable:          true,
			ReplicaID:       cyclicReplicaID,
			FilterReplicaID: filter,
			SyncDDL:         cyclicSyncDDL,
			// TODO(neil) enable ID bucket.
		}
	}

	if !cfg.EnableOldValue {
		sinkURIParsed, err := url.Parse(sinkURI)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}

		protocol := sinkURIParsed.Query().Get("protocol")
		for _, fp := range forceEnableOldValueProtocols {
			if protocol == fp {
				log.Warn("Attemping to replicate without old value enabled. CDC will enable old value and continue.", zap.String("protocol", protocol))
				cfg.EnableOldValue = true
				break
			}
		}

		if cfg.ForceReplicate {
			log.Error("if use force replicate, old value feature must be enabled")
			return nil, cerror.ErrOldValueNotEnabled.GenWithStackByArgs()
		}
	}

	for _, rules := range cfg.Sink.DispatchRules {
		switch strings.ToLower(rules.Dispatcher) {
		case "rowid", "index-value":
			if cfg.EnableOldValue {
				cmd.Printf("[WARN] This index-value distribution mode "+
					"does not guarantee row-level orderliness when "+
					"switching on the old value, so please use caution! dispatch-rules: %#v", rules)
			}
		}
	}
	switch sortEngine {
	case model.SortUnified, model.SortInMemory, model.SortInFile:
	default:
		return nil, errors.Errorf("Creating chengfeed with an invalid sort engine(%s), `%s`,`%s` and `%s` are optional.", sortEngine, model.SortUnified, model.SortInMemory, model.SortInFile)
	}
	info := &model.ChangeFeedInfo{
		SinkURI:           sinkURI,
		Opts:              make(map[string]string),
		CreateTime:        time.Now(),
		StartTs:           startTs,
		TargetTs:          targetTs,
		Config:            cfg,
		Engine:            sortEngine,
		State:             model.StateNormal,
		SyncPointEnabled:  syncPointEnabled,
		SyncPointInterval: syncPointInterval,
		CreatorVersion:    version.ReleaseVersion,
	}

	if sortDir != "" {
		cmd.Printf("[WARN] --sort-dir is deprecated in changefeed settings. " +
			"Please use `cdc server --sort-dir` if possible. " +
			"The sort-dir here will be no-op\n")
	}

	if info.Engine == model.SortInFile {
		cmd.Printf("[WARN] file sorter is deprecated. " +
			"make sure that you DO NOT use it in production. " +
			"Adjust \"sort-engine\" to make use of the right sorter.\n")
	}

	tz, err := util.GetTimezone(timezone)
	if err != nil {
		return nil, errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}

	if isCreate {
		ctx = util.PutTimezoneInCtx(ctx, tz)
		ineligibleTables, eligibleTables, err := verifyTables(ctx, credential, cfg, startTs)
		if err != nil {
			return nil, err
		}
		if len(ineligibleTables) != 0 {
			if cfg.ForceReplicate {
				cmd.Printf("[WARN] force to replicate some ineligible tables, %#v\n", ineligibleTables)
			} else {
				cmd.Printf("[WARN] some tables are not eligible to replicate, %#v\n", ineligibleTables)
				if !noConfirm {
					cmd.Printf("Could you agree to ignore those tables, and continue to replicate [Y/N]\n")
					var yOrN string
					_, err := fmt.Scan(&yOrN)
					if err != nil {
						return nil, err
					}
					if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
						cmd.Printf("No changefeed is created because you don't want to ignore some tables.\n")
						return nil, nil
					}
				}
			}
		}
		if cfg.Cyclic.IsEnabled() && !cyclic.IsTablesPaired(eligibleTables) {
			return nil, errors.New("normal tables and mark tables are not paired, " +
				"please run `cdc cli changefeed cyclic create-marktables`")
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

	err = verifySink(ctx, info.SinkURI, info.Config, info.Opts)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func changefeedConfigVariables(command *cobra.Command) {
	command.PersistentFlags().Uint64Var(&startTs, "start-ts", 0, "Start ts of changefeed")
	command.PersistentFlags().Uint64Var(&targetTs, "target-ts", 0, "Target ts of changefeed")
	command.PersistentFlags().StringVar(&sinkURI, "sink-uri", "", "sink uri")
	command.PersistentFlags().StringVar(&configFile, "config", "", "Path of the configuration file")
	command.PersistentFlags().StringSliceVar(&opts, "opts", nil, "Extra options, in the `key=value` format")
	command.PersistentFlags().StringVar(&sortEngine, "sort-engine", model.SortUnified, "sort engine used for data sort")
	command.PersistentFlags().StringVar(&sortDir, "sort-dir", defaultSortDir, "directory used for data sort")
	command.PersistentFlags().StringVar(&timezone, "tz", "SYSTEM", "timezone used when checking sink uri (changefeed timezone is determined by cdc server)")
	command.PersistentFlags().Uint64Var(&cyclicReplicaID, "cyclic-replica-id", 0, "(Expremental) Cyclic replication replica ID of changefeed")
	command.PersistentFlags().UintSliceVar(&cyclicFilterReplicaIDs, "cyclic-filter-replica-ids", []uint{}, "(Expremental) Cyclic replication filter replica ID of changefeed")
	command.PersistentFlags().BoolVar(&cyclicSyncDDL, "cyclic-sync-ddl", true, "(Expremental) Cyclic replication sync DDL of changefeed")
	command.PersistentFlags().BoolVar(&syncPointEnabled, "sync-point", false, "(Expremental) Set and Record syncpoint in replication(default off)")
	command.PersistentFlags().DurationVar(&syncPointInterval, "sync-interval", 10*time.Minute, "(Expremental) Set the interval for syncpoint in replication(default 10min)")
	command.PersistentFlags().MarkHidden("sort-dir") //nolint:errcheck
}

func newCreateChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "create",
		Short: "Create a new replication task (changefeed)",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			id := changefeedID
			if id == "" {
				id = uuid.New().String()
			}

			_, captureInfos, err := cdcEtcdCli.GetCaptures(ctx)
			if err != nil {
				return err
			}
			info, err := verifyChangefeedParamers(ctx, cmd, true /* isCreate */, getCredential(), captureInfos)
			if err != nil {
				return err
			}
			if info == nil {
				return nil
			}

			infoStr, err := info.Marshal()
			if err != nil {
				return err
			}
			err = cdcEtcdCli.CreateChangefeedInfo(ctx, info, id)
			if err != nil {
				return err
			}
			cmd.Printf("Create changefeed successfully!\nID: %s\nInfo: %s\n", id, infoStr)
			return nil
		},
	}
	changefeedConfigVariables(command)
	command.PersistentFlags().BoolVar(&noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")
	command.PersistentFlags().StringVarP(&changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	command.PersistentFlags().BoolVarP(&disableGCSafePointCheck, "disable-gc-check", "", false, "Disable GC safe point check")

	return command
}

func newUpdateChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "update",
		Short: "Update config of an existing replication task (changefeed)",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := defaultContext

			old, err := cdcEtcdCli.GetChangeFeedInfo(ctx, changefeedID)
			if err != nil {
				return err
			}
			info, err := old.Clone()
			if err != nil {
				return err
			}

			cmd.Flags().Visit(func(flag *pflag.Flag) {
				switch flag.Name {
				case "target-ts":
					info.TargetTs = targetTs
				case "sink-uri":
					info.SinkURI = sinkURI
				case "config":
					cfg := info.Config
					if err = strictDecodeFile(configFile, "TiCDC changefeed", cfg); err != nil {
						log.Error("decode config file error", zap.Error(err))
					}
				case "opts":
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

				case "sort-engine":
					info.Engine = sortEngine
				case "cyclic-replica-id":
					filter := make([]uint64, 0, len(cyclicFilterReplicaIDs))
					for _, id := range cyclicFilterReplicaIDs {
						filter = append(filter, uint64(id))
					}
					info.Config.Cyclic.FilterReplicaID = filter
				case "cyclic-sync-ddl":
					info.Config.Cyclic.SyncDDL = cyclicSyncDDL
				case "sync-point":
					info.SyncPointEnabled = syncPointEnabled
				case "sync-interval":
					info.SyncPointInterval = syncPointInterval
				case "pd", "tz", "start-ts", "changefeed-id", "no-confirm":
					// do nothing
				default:
					// use this default branch to prevent new added parameter is not added
					log.Warn("unsupported flag, please report a bug", zap.String("flagName", flag.Name))
				}
			})
			if err != nil {
				return err
			}

			resp, err := applyOwnerChangefeedQuery(ctx, changefeedID, getCredential())
			// if no cdc owner exists, allow user to update changefeed config
			if err != nil && errors.Cause(err) != errOwnerNotFound {
				return err
			}
			// Note that the correctness of the logic here depends on the return value of `/capture/owner/changefeed/query` interface.
			// TODO: Using error codes instead of string containing judgments
			if err == nil && !strings.Contains(resp, `"state": "stopped"`) {
				return errors.Errorf("can only update changefeed config when it is stopped\nstatus: %s", resp)
			}

			changelog, err := diff.Diff(old, info)
			if err != nil {
				return err
			}
			if len(changelog) == 0 {
				cmd.Printf("changefeed config is the same with the old one, do nothing\n")
				return nil
			}
			cmd.Printf("Diff of changefeed config:\n")
			for _, change := range changelog {
				cmd.Printf("%+v\n", change)
			}

			if !noConfirm {
				cmd.Printf("Could you agree to apply changes above to changefeed [Y/N]\n")
				var yOrN string
				_, err = fmt.Scan(&yOrN)
				if err != nil {
					return err
				}
				if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
					cmd.Printf("No upadte to changefeed.\n")
					return nil
				}
			}

			err = cdcEtcdCli.SaveChangeFeedInfo(ctx, info, changefeedID)
			if err != nil {
				return err
			}
			infoStr, err := info.Marshal()
			if err != nil {
				return err
			}
			cmd.Printf("Update changefeed config successfully! "+
				"Will take effect only if the changefeed has been paused before this command"+
				"\nID: %s\nInfo: %s\n", changefeedID, infoStr)
			return nil
		},
	}
	changefeedConfigVariables(command)
	command.PersistentFlags().StringVarP(&changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	command.PersistentFlags().BoolVar(&noConfirm, "no-confirm", false, "Don't ask user whether to confirm update changefeed config")
	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}

func newStatisticsChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "statistics",
		Short: "Periodically check and output the status of a replicaiton task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			tick := time.NewTicker(time.Duration(interval) * time.Second)
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
					status, _, err := cdcEtcdCli.GetChangeFeedStatus(ctx, changefeedID)
					if err != nil {
						return err
					}
					taskPositions, err := cdcEtcdCli.GetAllTaskPositions(ctx, changefeedID)
					if err != nil {
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
	command.PersistentFlags().StringVarP(&changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	command.PersistentFlags().UintVarP(&interval, "interval", "I", 10, "Interval for outputing the latest statistics")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	return command
}

func newCreateChangefeedCyclicCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "cyclic",
		Short: "(Expremental) Utility about cyclic replication",
	}
	command.AddCommand(
		&cobra.Command{
			Use:   "create-marktables",
			Short: "Create cyclic replication mark tables",
			Long:  ``,
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := defaultContext

				cfg := config.GetDefaultReplicaConfig()
				if len(configFile) > 0 {
					if err := strictDecodeFile(configFile, "TiCDC changefeed", cfg); err != nil {
						return err
					}
				}
				ts, logical, err := pdCli.GetTS(ctx)
				if err != nil {
					return err
				}
				startTs = oracle.ComposeTS(ts, logical)

				_, eligibleTables, err := verifyTables(ctx, getCredential(), cfg, startTs)
				if err != nil {
					return err
				}
				tables := make([]mark.TableName, len(eligibleTables))
				for i := range eligibleTables {
					tables[i] = &eligibleTables[i]
				}
				err = mark.CreateMarkTables(ctx, cyclicUpstreamDSN, getUpstreamCredential(), tables...)
				if err != nil {
					return err
				}
				cmd.Printf("Create cyclic replication mark tables successfully! Total tables: %d\n", len(eligibleTables))
				return nil
			},
		})
	command.PersistentFlags().StringVar(&cyclicUpstreamDSN, "cyclic-upstream-dsn", "", "(Expremental) Upsteam TiDB DSN in the form of [user[:password]@][net[(addr)]]/")
	command.PersistentFlags().StringVar(&upstreamSslCaPath, "cyclic-upstream-ssl-ca", "", "CA certificate path for TLS connection")
	command.PersistentFlags().StringVar(&upstreamSslCertPath, "cyclic-upstream-ssl-cert", "", "Certificate path for TLS connection")
	command.PersistentFlags().StringVar(&upstreamSslKeyPath, "cyclic-upstream-ssl-key", "", "Private key path for TLS connection")

	return command
}

var (
	upstreamSslCaPath   string
	upstreamSslCertPath string
	upstreamSslKeyPath  string
)

func getUpstreamCredential() *security.Credential {
	return &security.Credential{
		CAPath:   upstreamSslCaPath,
		CertPath: upstreamSslCertPath,
		KeyPath:  upstreamSslKeyPath,
	}
}
