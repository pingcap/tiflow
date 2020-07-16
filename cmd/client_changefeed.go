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
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/r3labs/diff"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
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
				return applyAdminChangefeed(ctx, job)
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
				return applyAdminChangefeed(ctx, job)
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
				}
				return applyAdminChangefeed(ctx, job)
			},
		},
	}

	for _, cmd := range cmds {
		cmd.PersistentFlags().StringVarP(&changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
		_ = cmd.MarkPersistentFlagRequired("changefeed-id")
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
			cfs := make([]*changefeedCommonInfo, 0, len(raw))
			for id := range raw {
				cfci := &changefeedCommonInfo{ID: id}
				resp, err := applyOwnerChangefeedQuery(ctx, id)
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
	return command
}

func newQueryChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "query",
		Short: "Query information and status of a replicaiton task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext

			if simplified {
				resp, err := applyOwnerChangefeedQuery(ctx, changefeedID)
				if err != nil {
					return err
				}
				cmd.Println(resp)
				return nil
			}

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
			if info == nil {
				log.Warn("this changefeed has been deleted, the residual meta data will be completely deleted within 24 hours.")
			}
			return jsonPrint(cmd, meta)
		},
	}
	command.PersistentFlags().BoolVarP(&simplified, "simple", "s", false, "Output simplified replication status")
	command.PersistentFlags().StringVarP(&changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = command.MarkPersistentFlagRequired("changefeed-id")
	return command
}

func verifyChangefeedParamers(ctx context.Context, cmd *cobra.Command, isCreate bool, credential *security.Credential) (*model.ChangeFeedInfo, error) {
	if isCreate {
		if startTs == 0 {
			ts, logical, err := pdCli.GetTS(ctx)
			if err != nil {
				return nil, err
			}
			startTs = oracle.ComposeTS(ts, logical)
		}
		if err := verifyStartTs(ctx, startTs, cdcEtcdCli); err != nil {
			return nil, err
		}
	}

	cfg := config.GetDefaultReplicaConfig()
	if len(configFile) > 0 {
		if err := strictDecodeFile(configFile, "cdc", cfg); err != nil {
			return nil, err
		}
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
	info := &model.ChangeFeedInfo{
		SinkURI:    sinkURI,
		Opts:       make(map[string]string),
		CreateTime: time.Now(),
		StartTs:    startTs,
		TargetTs:   targetTs,
		Config:     cfg,
		Engine:     model.SortEngine(sortEngine),
		SortDir:    sortDir,
		State:      model.StateNormal,
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
	command.PersistentFlags().StringVar(&sinkURI, "sink-uri", "mysql://root:123456@127.0.0.1:3306/", "sink uri")
	command.PersistentFlags().StringVar(&configFile, "config", "", "Path of the configuration file")
	command.PersistentFlags().StringSliceVar(&opts, "opts", nil, "Extra options, in the `key=value` format")
	command.PersistentFlags().StringVar(&sortEngine, "sort-engine", "memory", "sort engine used for data sort")
	command.PersistentFlags().StringVar(&sortDir, "sort-dir", ".", "directory used for file sort")
	command.PersistentFlags().StringVar(&timezone, "tz", "SYSTEM", "timezone used when checking sink uri (changefeed timezone is determined by cdc server)")
	command.PersistentFlags().Uint64Var(&cyclicReplicaID, "cyclic-replica-id", 0, "(Expremental) Cyclic replication replica ID of changefeed")
	command.PersistentFlags().UintSliceVar(&cyclicFilterReplicaIDs, "cyclic-filter-replica-ids", []uint{}, "(Expremental) Cyclic replication filter replica ID of changefeed")
	command.PersistentFlags().BoolVar(&cyclicSyncDDL, "cyclic-sync-ddl", true, "(Expremental) Cyclic replication sync DDL of changefeed")
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

			info, err := verifyChangefeedParamers(ctx, cmd, true /* isCreate */, getCredential())
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

	return command
}

func newUpdateChangefeedCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "update",
		Short: "Update config of an existing replication task (changefeed)",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext

			old, err := cdcEtcdCli.GetChangeFeedInfo(ctx, changefeedID)
			if err != nil {
				return err
			}

			info, err := verifyChangefeedParamers(ctx, cmd, false /* isCreate */, getCredential())
			if err != nil {
				return err
			}
			// Fix some fields that can't be updated.
			info.CreateTime = old.CreateTime
			info.AdminJobType = old.AdminJobType

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
					if err := strictDecodeFile(configFile, "cdc", cfg); err != nil {
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
