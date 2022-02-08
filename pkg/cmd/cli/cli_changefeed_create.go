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
	"net/url"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil/gc"
	ticdcutil "github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// changefeedCommonOptions defines common changefeed flags.
type changefeedCommonOptions struct {
	noConfirm              bool
	targetTs               uint64
	sinkURI                string
	configFile             string
	opts                   []string
	sortEngine             string
	sortDir                string
	cyclicReplicaID        uint64
	cyclicFilterReplicaIDs []uint
	cyclicSyncDDL          bool
	syncPointEnabled       bool
	syncPointInterval      time.Duration
}

// newChangefeedCommonOptions creates new changefeed common options.
func newChangefeedCommonOptions() *changefeedCommonOptions {
	return &changefeedCommonOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *changefeedCommonOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	cmd.PersistentFlags().BoolVar(&o.noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")
	cmd.PersistentFlags().Uint64Var(&o.targetTs, "target-ts", 0, "Target ts of changefeed")
	cmd.PersistentFlags().StringVar(&o.sinkURI, "sink-uri", "", "sink uri")
	cmd.PersistentFlags().StringVar(&o.configFile, "config", "", "Path of the configuration file")
	cmd.PersistentFlags().StringSliceVar(&o.opts, "opts", nil, "Extra options, in the `key=value` format")
	cmd.PersistentFlags().StringVar(&o.sortEngine, "sort-engine", model.SortUnified, "sort engine used for data sort")
	cmd.PersistentFlags().StringVar(&o.sortDir, "sort-dir", "", "directory used for data sort")
	cmd.PersistentFlags().Uint64Var(&o.cyclicReplicaID, "cyclic-replica-id", 0, "(Experimental) Cyclic replication replica ID of changefeed")
	cmd.PersistentFlags().UintSliceVar(&o.cyclicFilterReplicaIDs, "cyclic-filter-replica-ids", []uint{}, "(Experimental) Cyclic replication filter replica ID of changefeed")
	cmd.PersistentFlags().BoolVar(&o.cyclicSyncDDL, "cyclic-sync-ddl", true, "(Experimental) Cyclic replication sync DDL of changefeed")
	cmd.PersistentFlags().BoolVar(&o.syncPointEnabled, "sync-point", false, "(Experimental) Set and Record syncpoint in replication(default off)")
	cmd.PersistentFlags().DurationVar(&o.syncPointInterval, "sync-interval", 10*time.Minute, "(Experimental) Set the interval for syncpoint in replication(default 10min)")
	_ = cmd.PersistentFlags().MarkHidden("sort-dir")
}

// strictDecodeConfig do strictDecodeFile check and only verify the rules for now.
func (o *changefeedCommonOptions) strictDecodeConfig(component string, cfg *config.ReplicaConfig) error {
	err := util.StrictDecodeFile(o.configFile, component, cfg)
	if err != nil {
		return err
	}

	_, err = filter.VerifyRules(cfg)

	return err
}

// createChangefeedOptions defines common flags for the `cli changefeed crate` command.
type createChangefeedOptions struct {
	commonChangefeedOptions *changefeedCommonOptions

	etcdClient *etcd.CDCEtcdClient
	pdClient   pd.Client

	pdAddr     string
	credential *security.Credential

	changefeedID            string
	disableGCSafePointCheck bool
	startTs                 uint64
	timezone                string

	cfg *config.ReplicaConfig
}

// newCreateChangefeedOptions creates new options for the `cli changefeed create` command.
func newCreateChangefeedOptions(commonChangefeedOptions *changefeedCommonOptions) *createChangefeedOptions {
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
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().BoolVarP(&o.disableGCSafePointCheck, "disable-gc-check", "", false, "Disable GC safe point check")
	cmd.PersistentFlags().Uint64Var(&o.startTs, "start-ts", 0, "Start ts of changefeed")
	cmd.PersistentFlags().StringVar(&o.timezone, "tz", "SYSTEM", "timezone used when checking sink uri (changefeed timezone is determined by cdc server)")
}

// complete adapts from the command line args to the data and client required.
func (o *createChangefeedOptions) complete(ctx context.Context, f factory.Factory, cmd *cobra.Command) error {
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

	o.pdAddr = f.GetPdAddr()
	o.credential = f.GetCredential()

	if o.startTs == 0 {
		ts, logical, err := o.pdClient.GetTS(ctx)
		if err != nil {
			return err
		}
		o.startTs = oracle.ComposeTS(ts, logical)
	}
	_, captureInfos, err := o.etcdClient.GetCaptures(ctx)
	if err != nil {
		return err
	}

	return o.completeCfg(cmd, captureInfos)
}

// completeCfg complete the replica config from file and cmd flags.
func (o *createChangefeedOptions) completeCfg(
	cmd *cobra.Command, captureInfos []*model.CaptureInfo,
) error {
	cdcClusterVer, err := version.GetTiCDCClusterVersion(model.ListVersionsFromCaptureInfos(captureInfos))
	if err != nil {
		return errors.Trace(err)
	}

	cfg := config.GetDefaultReplicaConfig()
	if len(o.commonChangefeedOptions.configFile) > 0 {
		if err := o.commonChangefeedOptions.strictDecodeConfig("TiCDC changefeed", cfg); err != nil {
			return err
		}
	}

	if !cdcClusterVer.ShouldEnableOldValueByDefault() {
		cfg.EnableOldValue = false
		log.Warn("The TiCDC cluster is built from an older version, disabling old value by default.",
			zap.String("version", cdcClusterVer.String()))
	}

	if !cfg.EnableOldValue {
		sinkURIParsed, err := url.Parse(o.commonChangefeedOptions.sinkURI)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}

		protocol := sinkURIParsed.Query().Get(config.ProtocolKey)
		if protocol != "" {
			cfg.Sink.Protocol = protocol
		}
		for _, fp := range config.ForceEnableOldValueProtocols {
			if cfg.Sink.Protocol == fp {
				log.Warn("Attempting to replicate without old value enabled. CDC will enable old value and continue.", zap.String("protocol", cfg.Sink.Protocol))
				cfg.EnableOldValue = true
				break
			}
		}

		if cfg.ForceReplicate {
			log.Error("if use force replicate, old value feature must be enabled")
			return cerror.ErrOldValueNotEnabled.GenWithStackByArgs()
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
	case model.SortInMemory:
	case model.SortInFile:
	case model.SortUnified:
	default:
		log.Warn("invalid sort-engine, use Unified Sorter by default",
			zap.String("invalidSortEngine", o.commonChangefeedOptions.sortEngine))
		o.commonChangefeedOptions.sortEngine = model.SortUnified
	}

	if o.commonChangefeedOptions.sortEngine == model.SortUnified && !cdcClusterVer.ShouldEnableUnifiedSorterByDefault() {
		o.commonChangefeedOptions.sortEngine = model.SortInMemory
		log.Warn("The TiCDC cluster is built from an older version, disabling Unified Sorter by default",
			zap.String("version", cdcClusterVer.String()))
	}

	if o.disableGCSafePointCheck {
		cfg.CheckGCSafePoint = false
	}

	if o.commonChangefeedOptions.cyclicReplicaID != 0 || len(o.commonChangefeedOptions.cyclicFilterReplicaIDs) != 0 {
		if !(o.commonChangefeedOptions.cyclicReplicaID != 0 && len(o.commonChangefeedOptions.cyclicFilterReplicaIDs) != 0) {
			return errors.New("invalid cyclic config, please make sure using " +
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
	// Complete cfg.
	o.cfg = cfg

	return nil
}

// validate checks that the provided attach options are specified.
func (o *createChangefeedOptions) validate(ctx context.Context, cmd *cobra.Command) error {
	if o.commonChangefeedOptions.sinkURI == "" {
		return errors.New("Creating changefeed without a sink-uri")
	}

	err := o.cfg.Validate()
	if err != nil {
		return err
	}

	if err := o.validateStartTs(ctx); err != nil {
		return err
	}

	if err := o.validateTargetTs(); err != nil {
		return err
	}

	// user is not allowed to set sort-dir at changefeed level
	if o.commonChangefeedOptions.sortDir != "" {
		cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in changefeed settings. " +
			"Please use `cdc server --data-dir` to start the cdc server if possible, sort-dir will be set automatically. " +
			"The --sort-dir here will be no-op\n"))
		return errors.New("Creating changefeed with `--sort-dir`, it's invalid")
	}

	switch o.commonChangefeedOptions.sortEngine {
	case model.SortUnified, model.SortInMemory:
	case model.SortInFile:
		// obsolete. But we keep silent here. We create a Unified Sorter when the owner/processor sees this option
		// for backward-compatibility.
	default:
		return errors.Errorf("Creating changefeed with an invalid sort engine(%s), "+
			"`%s` and `%s` are the only valid options.", o.commonChangefeedOptions.sortEngine, model.SortUnified, model.SortInMemory)
	}

	return nil
}

// getInfo constructs the information for the changefeed.
func (o *createChangefeedOptions) getInfo(cmd *cobra.Command) *model.ChangeFeedInfo {
	info := &model.ChangeFeedInfo{
		SinkURI:           o.commonChangefeedOptions.sinkURI,
		Opts:              make(map[string]string),
		CreateTime:        time.Now(),
		StartTs:           o.startTs,
		TargetTs:          o.commonChangefeedOptions.targetTs,
		Config:            o.cfg,
		Engine:            o.commonChangefeedOptions.sortEngine,
		State:             model.StateNormal,
		SyncPointEnabled:  o.commonChangefeedOptions.syncPointEnabled,
		SyncPointInterval: o.commonChangefeedOptions.syncPointInterval,
		CreatorVersion:    version.ReleaseVersion,
	}

	if info.Engine == model.SortInFile {
		cmd.Printf("[WARN] file sorter is deprecated. " +
			"make sure that you DO NOT use it in production. " +
			"Adjust \"sort-engine\" to make use of the right sorter.\n")
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

	return info
}

// validateStartTs checks if startTs is a valid value.
func (o *createChangefeedOptions) validateStartTs(ctx context.Context) error {
	if o.disableGCSafePointCheck {
		return nil
	}
	// Ensure the start ts is validate in the next 1 hour.
	const ensureTTL = 60 * 60.
	return gc.EnsureChangefeedStartTsSafety(
		ctx, o.pdClient, o.changefeedID, ensureTTL, o.startTs)
}

// validateTargetTs checks if targetTs is a valid value.
func (o *createChangefeedOptions) validateTargetTs() error {
	if o.commonChangefeedOptions.targetTs > 0 && o.commonChangefeedOptions.targetTs <= o.startTs {
		return errors.Errorf("target-ts %d must be larger than start-ts: %d", o.commonChangefeedOptions.targetTs, o.startTs)
	}
	return nil
}

// validateSink will create a sink and verify that the configuration is correct.
func (o *createChangefeedOptions) validateSink(
	ctx context.Context, cfg *config.ReplicaConfig, opts map[string]string,
) error {
	return sink.Validate(ctx, o.commonChangefeedOptions.sinkURI, cfg, opts)
}

// run the `cli changefeed create` command.
func (o *createChangefeedOptions) run(ctx context.Context, cmd *cobra.Command) error {
	id := o.changefeedID
	if id == "" {
		id = uuid.New().String()
	}
	if err := model.ValidateChangefeedID(id); err != nil {
		return err
	}

	if !o.commonChangefeedOptions.noConfirm {
		currentPhysical, _, err := o.pdClient.GetTS(ctx)
		if err != nil {
			return err
		}

		if err := confirmLargeDataGap(cmd, currentPhysical, o.startTs); err != nil {
			return err
		}
	}

	ineligibleTables, eligibleTables, err := getTables(o.pdAddr, o.credential, o.cfg, o.startTs)
	if err != nil {
		return err
	}

	if len(ineligibleTables) != 0 {
		if o.cfg.ForceReplicate {
			cmd.Printf("[WARN] force to replicate some ineligible tables, %#v\n", ineligibleTables)
		} else {
			cmd.Printf("[WARN] some tables are not eligible to replicate, %#v\n", ineligibleTables)
			if !o.commonChangefeedOptions.noConfirm {
				if err := confirmIgnoreIneligibleTables(cmd); err != nil {
					return err
				}
			}
		}
	}

	if o.cfg.Cyclic.IsEnabled() && !cyclic.IsTablesPaired(eligibleTables) {
		return errors.New("normal tables and mark tables are not paired, " +
			"please run `cdc cli changefeed cyclic create-marktables`")
	}

	info := o.getInfo(cmd)

	tz, err := ticdcutil.GetTimezone(o.timezone)
	if err != nil {
		return errors.Annotate(err, "can not load timezone, Please specify the time zone through environment variable `TZ` or command line parameters `--tz`")
	}

	ctx = ticdcutil.PutTimezoneInCtx(ctx, tz)
	err = o.validateSink(ctx, info.Config, info.Opts)
	if err != nil {
		return err
	}

	infoStr, err := info.Marshal()
	if err != nil {
		return err
	}

	err = o.etcdClient.CreateChangefeedInfo(ctx, info, id)
	if err != nil {
		return err
	}

	cmd.Printf("Create changefeed successfully!\nID: %s\nInfo: %s\n", id, infoStr)

	return nil
}

// newCmdCreateChangefeed creates the `cli changefeed create` command.
func newCmdCreateChangefeed(f factory.Factory) *cobra.Command {
	commonChangefeedOptions := newChangefeedCommonOptions()

	o := newCreateChangefeedOptions(commonChangefeedOptions)

	command := &cobra.Command{
		Use:   "create",
		Short: "Create a new replication task (changefeed)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmdcontext.GetDefaultContext()

			err := o.complete(ctx, f, cmd)
			if err != nil {
				return err
			}

			err = o.validate(ctx, cmd)
			if err != nil {
				return err
			}

			return o.run(ctx, cmd)
		},
	}

	o.addFlags(command)

	return command
}
