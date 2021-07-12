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
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/pingcap/ticdc/pkg/cmd/cli"
	cmdcontext "github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/security"
	ticdcutil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var forceEnableOldValueProtocols = []string{
	"canal",
	"maxwell",
}

// commonChangefeedOptions defines common changefeed flags.
type commonChangefeedOptions struct {
	options *cli.Options

	startTs                uint64
	targetTs               uint64
	sinkURI                string
	configFile             string
	opts                   []string
	sortEngine             string
	sortDir                string
	timezone               string
	cyclicReplicaID        uint64
	cyclicFilterReplicaIDs []uint
	cyclicSyncDDL          bool
	syncPointEnabled       bool
	syncPointInterval      time.Duration
}

// newCommonChangefeedOptions creates new common changefeed options.
func newCommonChangefeedOptions(options *cli.Options) *commonChangefeedOptions {
	return &commonChangefeedOptions{
		options: options,
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *commonChangefeedOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.PersistentFlags().Uint64Var(&o.startTs, "start-ts", 0, "Start ts of changefeed")
	cmd.PersistentFlags().Uint64Var(&o.targetTs, "target-ts", 0, "Target ts of changefeed")
	cmd.PersistentFlags().StringVar(&o.sinkURI, "sink-uri", "", "sink uri")
	cmd.PersistentFlags().StringVar(&o.configFile, "config", "", "Path of the configuration file")
	cmd.PersistentFlags().StringSliceVar(&o.opts, "opts", nil, "Extra options, in the `key=value` format")
	cmd.PersistentFlags().StringVar(&o.sortEngine, "sort-engine", model.SortUnified, "sort engine used for data sort")
	cmd.PersistentFlags().StringVar(&o.sortDir, "sort-dir", "", "directory used for data sort")
	cmd.PersistentFlags().StringVar(&o.timezone, "tz", "SYSTEM", "timezone used when checking sink uri (changefeed timezone is determined by cdc server)")
	cmd.PersistentFlags().Uint64Var(&o.cyclicReplicaID, "cyclic-replica-id", 0, "(Experimental) Cyclic replication replica ID of changefeed")
	cmd.PersistentFlags().UintSliceVar(&o.cyclicFilterReplicaIDs, "cyclic-filter-replica-ids", []uint{}, "(Experimental) Cyclic replication filter replica ID of changefeed")
	cmd.PersistentFlags().BoolVar(&o.cyclicSyncDDL, "cyclic-sync-ddl", true, "(Experimental) Cyclic replication sync DDL of changefeed")
	cmd.PersistentFlags().BoolVar(&o.syncPointEnabled, "sync-point", false, "(Experimental) Set and Record syncpoint in replication(default off)")
	cmd.PersistentFlags().DurationVar(&o.syncPointInterval, "sync-interval", 10*time.Minute, "(Experimental) Set the interval for syncpoint in replication(default 10min)")
	_ = cmd.PersistentFlags().MarkHidden("sort-dir") //nolint:errcheck
}

// validateReplicaConfig do strictDecodeFile check and only verify the rules for now.
func (o *commonChangefeedOptions) validateReplicaConfig(component string, cfg *config.ReplicaConfig) error {
	err := util.StrictDecodeFile(o.configFile, component, cfg)
	if err != nil {
		return err
	}
	_, err = filter.VerifyRules(cfg)
	return err
}

func (o *commonChangefeedOptions) validateTables(credential *security.Credential, cfg *config.ReplicaConfig) (ineligibleTables, eligibleTables []model.TableName, err error) {
	kvStore, err := kv.CreateTiStore(o.options.CliPdAddr, credential)
	if err != nil {
		return nil, nil, err
	}
	meta, err := kv.GetSnapshotMeta(kvStore, o.startTs)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	filter, err := filter.NewFilter(cfg)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	snap, err := entry.NewSingleSchemaSnapshotFromMeta(meta, o.startTs, false /* explicitTables */)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	for tID, tableName := range snap.CloneTables() {
		tableInfo, exist := snap.TableByID(tID)
		if !exist {
			return nil, nil, errors.NotFoundf("table %d", tID)
		}
		if filter.ShouldIgnoreTable(tableName.Schema, tableName.Table) {
			continue
		}
		if !tableInfo.IsEligible(false /* forceReplicate */) {
			ineligibleTables = append(ineligibleTables, tableName)
		} else {
			eligibleTables = append(eligibleTables, tableName)
		}
	}
	return
}

// createChangefeedOptions defines common flags for the `cli changefeed crate` command.
type createChangefeedOptions struct {
	disableGCSafePointCheck bool

	commonChangefeedOptions *commonChangefeedOptions
}

// newCreateChangefeedOptions creates new options for the `cli changefeed create` command.
func newCreateChangefeedOptions(commonChangefeedOptions *commonChangefeedOptions) *createChangefeedOptions {
	return &createChangefeedOptions{
		commonChangefeedOptions: commonChangefeedOptions,
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *createChangefeedOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	o.commonChangefeedOptions.addFlags(cmd)
	cmd.PersistentFlags().BoolVarP(&o.disableGCSafePointCheck, "disable-gc-check", "", false, "Disable GC safe point check")
}

// newCmdCreateChangefeed creates the `cli changefeed create` command.
func newCmdCreateChangefeed(f util.Factory, options *cli.Options, commonOptions *commonOptions) *cobra.Command {
	commonChangefeedOptions := newCommonChangefeedOptions(options)
	o := newCreateChangefeedOptions(commonChangefeedOptions)

	command := &cobra.Command{
		Use:   "create",
		Short: "Create a new replication task (changefeed)",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()

			id := commonOptions.changefeedID
			if id == "" {
				id = uuid.New().String()
			}
			// validate the changefeedID first
			if err := model.ValidateChangefeedID(id); err != nil {
				return err
			}

			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			pdClient, err := f.PdClient()
			if err != nil {
				return err
			}

			_, captureInfos, err := etcdClient.GetCaptures(ctx)
			if err != nil {
				return err
			}

			info, err := o.validate(ctx, pdClient, commonOptions, cmd, true /* isCreate */, f.GetCredential(), captureInfos)
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

			err = etcdClient.CreateChangefeedInfo(ctx, info, id)
			if err != nil {
				return err
			}

			cmd.Printf("Create changefeed successfully!\nID: %s\nInfo: %s\n", id, infoStr)

			return nil
		},
	}

	o.addFlags(command)
	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}

func (o *createChangefeedOptions) validate(ctx context.Context, pdClient pd.Client, commonOptions *commonOptions, cmd *cobra.Command, isCreate bool, credential *security.Credential, captureInfos []*model.CaptureInfo) (*model.ChangeFeedInfo, error) {
	if isCreate {
		if o.commonChangefeedOptions.sinkURI == "" {
			return nil, errors.New("Creating changefeed without a sink-uri")
		}
		if o.commonChangefeedOptions.startTs == 0 {
			ts, logical, err := pdClient.GetTS(ctx)
			if err != nil {
				return nil, err
			}
			o.commonChangefeedOptions.startTs = oracle.ComposeTS(ts, logical)
		}
		if err := o.validateStartTs(ctx, pdClient, commonOptions.changefeedID); err != nil {
			return nil, err
		}
		if err := confirmLargeDataGap(ctx, pdClient, cmd, commonOptions, o.commonChangefeedOptions.startTs); err != nil {
			return nil, err
		}
		if err := o.validateTargetTs(); err != nil {
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
			o.commonChangefeedOptions.sortEngine = model.SortInMemory
		}
		log.Warn("The TiCDC cluster is built from 4.0-release branch, the old-value and unified-sorter are disabled by default.")
	}
	if len(o.commonChangefeedOptions.configFile) > 0 {
		if err := o.commonChangefeedOptions.validateReplicaConfig("TiCDC changefeed", cfg); err != nil {
			return nil, err
		}
	}
	if o.disableGCSafePointCheck {
		cfg.CheckGCSafePoint = false
	}
	if o.commonChangefeedOptions.cyclicReplicaID != 0 || len(o.commonChangefeedOptions.cyclicFilterReplicaIDs) != 0 {
		if !(o.commonChangefeedOptions.cyclicReplicaID != 0 && len(o.commonChangefeedOptions.cyclicFilterReplicaIDs) != 0) {
			return nil, errors.New("invalid cyclic config, please make sure using " +
				"nonzero replica ID and specify filter replica IDs")
		}
		filter := make([]uint64, 0, len(o.commonChangefeedOptions.cyclicFilterReplicaIDs))
		for _, id := range o.commonChangefeedOptions.cyclicFilterReplicaIDs {
			filter = append(filter, uint64(id))
		}
		cfg.Cyclic = &config.CyclicConfig{
			Enable:          true,
			ReplicaID:       o.commonChangefeedOptions.cyclicReplicaID,
			FilterReplicaID: filter,
			SyncDDL:         o.commonChangefeedOptions.cyclicSyncDDL,
			// TODO(neil) enable ID bucket.
		}
	}

	if !cfg.EnableOldValue {
		sinkURIParsed, err := url.Parse(o.commonChangefeedOptions.sinkURI)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}

		protocol := sinkURIParsed.Query().Get("protocol")
		for _, fp := range forceEnableOldValueProtocols {
			if protocol == fp {
				log.Warn("Attempting to replicate without old value enabled. CDC will enable old value and continue.", zap.String("protocol", protocol))
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
	switch o.commonChangefeedOptions.sortEngine {
	case model.SortUnified, model.SortInMemory, model.SortInFile:
	default:
		return nil, errors.Errorf("Creating changefeed with an invalid sort engine(%s), "+
			"`%s`,`%s` and `%s` are optional.", o.commonChangefeedOptions.sortEngine, model.SortUnified, model.SortInMemory, model.SortInFile)
	}
	info := &model.ChangeFeedInfo{
		SinkURI:           o.commonChangefeedOptions.sinkURI,
		Opts:              make(map[string]string),
		CreateTime:        time.Now(),
		StartTs:           o.commonChangefeedOptions.startTs,
		TargetTs:          o.commonChangefeedOptions.targetTs,
		Config:            cfg,
		Engine:            o.commonChangefeedOptions.sortEngine,
		State:             model.StateNormal,
		SyncPointEnabled:  o.commonChangefeedOptions.syncPointEnabled,
		SyncPointInterval: o.commonChangefeedOptions.syncPointInterval,
		CreatorVersion:    version.ReleaseVersion,
	}

	// user is not allowed to set sort-dir at changefeed level
	if o.commonChangefeedOptions.sortDir != "" {
		cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in changefeed settings. " +
			"Please use `cdc server --data-dir` to start the cdc server if possible, sort-dir will be set automatically. " +
			"The --sort-dir here will be no-op\n"))
		return nil, errors.New("Creating changefeed with `--sort-dir`, it's invalid")
	}

	if info.Engine == model.SortInFile {
		cmd.Printf("[WARN] file sorter is deprecated. " +
			"make sure that you DO NOT use it in production. " +
			"Adjust \"sort-engine\" to make use of the right sorter.\n")
	}

	tz, err := ticdcutil.GetTimezone(o.commonChangefeedOptions.timezone)
	if err != nil {
		return nil, errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}

	if isCreate {
		ctx = ticdcutil.PutTimezoneInCtx(ctx, tz)
		ineligibleTables, eligibleTables, err := o.commonChangefeedOptions.validateTables(credential, cfg)
		if err != nil {
			return nil, err
		}
		if len(ineligibleTables) != 0 {
			if cfg.ForceReplicate {
				cmd.Printf("[WARN] force to replicate some ineligible tables, %#v\n", ineligibleTables)
			} else {
				cmd.Printf("[WARN] some tables are not eligible to replicate, %#v\n", ineligibleTables)
				if !commonOptions.NoConfirm {
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

	for _, opt := range o.commonChangefeedOptions.opts {
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

	err = o.validateSink(ctx, info.Config, info.Opts)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (o *createChangefeedOptions) validateStartTs(ctx context.Context, pdClient pd.Client, changefeedID string) error {
	if o.disableGCSafePointCheck {
		return nil
	}

	return ticdcutil.CheckSafetyOfStartTs(ctx, pdClient, changefeedID, o.commonChangefeedOptions.startTs)
}

func (o *createChangefeedOptions) validateTargetTs() error {
	if o.commonChangefeedOptions.targetTs > 0 && o.commonChangefeedOptions.targetTs <= o.commonChangefeedOptions.startTs {
		return errors.Errorf("target-ts %d must be larger than start-ts: %d", o.commonChangefeedOptions.targetTs, o.commonChangefeedOptions.startTs)
	}
	return nil
}

func (o *createChangefeedOptions) validateSink(
	ctx context.Context, cfg *config.ReplicaConfig, opts map[string]string,
) error {
	filter, err := filter.NewFilter(cfg)
	if err != nil {
		return err
	}
	errCh := make(chan error)
	s, err := sink.NewSink(ctx, "cli-verify", o.commonChangefeedOptions.sinkURI, filter, cfg, opts, errCh)
	if err != nil {
		return err
	}
	err = s.Close()
	if err != nil {
		return err
	}
	select {
	case err = <-errCh:
		if err != nil {
			return err
		}
	default:
	}
	return nil
}
