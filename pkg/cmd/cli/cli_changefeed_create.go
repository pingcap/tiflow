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
	"net/url"
	"strings"

	"github.com/fatih/color"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/model"
	apiv2client "github.com/pingcap/tiflow/pkg/api/v2"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/pingcap/tiflow/pkg/cmd/util"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	putil "github.com/pingcap/tiflow/pkg/util"
	"github.com/spf13/cobra"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// changefeedCommonOptions defines common changefeed flags.
type changefeedCommonOptions struct {
	noConfirm      bool
	targetTs       uint64
	sinkURI        string
	schemaRegistry string
	configFile     string
	sortEngine     string
	sortDir        string

	upstreamPDAddrs  string
	upstreamCaPath   string
	upstreamCertPath string
	upstreamKeyPath  string
}

// newChangefeedCommonOptions creates new changefeed common options.
func newChangefeedCommonOptions() *changefeedCommonOptions {
	return &changefeedCommonOptions{}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *changefeedCommonOptions) addFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().BoolVar(&o.noConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")
	cmd.PersistentFlags().Uint64Var(&o.targetTs, "target-ts", 0, "Target ts of changefeed")
	cmd.PersistentFlags().StringVar(&o.sinkURI, "sink-uri", "", "sink uri")
	cmd.PersistentFlags().StringVar(&o.configFile, "config", "", "Path of the configuration file")
	cmd.PersistentFlags().StringVar(&o.sortEngine, "sort-engine", model.SortUnified, "sort engine used for data sort")
	cmd.PersistentFlags().StringVar(&o.sortDir, "sort-dir", "", "directory used for data sort")
	cmd.PersistentFlags().StringVar(&o.schemaRegistry, "schema-registry", "",
		"Avro Schema Registry URI")
	cmd.PersistentFlags().StringVar(&o.upstreamPDAddrs, "upstream-pd", "",
		"upstream PD address, use ',' to separate multiple PDs")
	cmd.PersistentFlags().StringVar(&o.upstreamCaPath, "upstream-ca", "",
		"CA certificate path for TLS connection to upstream")
	cmd.PersistentFlags().StringVar(&o.upstreamCertPath, "upstream-cert", "",
		"Certificate path for TLS connection to upstream")
	cmd.PersistentFlags().StringVar(&o.upstreamKeyPath, "upstream-key", "",
		"Private key path for TLS connection to upstream")
	_ = cmd.PersistentFlags().MarkHidden("sort-dir")
	// we don't support specify these flags below when cdc version >= 6.2.0
	_ = cmd.PersistentFlags().MarkHidden("sort-engine")
	// we don't support specify there flags below when cdc version <= 6.3.0
	_ = cmd.PersistentFlags().MarkHidden("upstream-pd")
	_ = cmd.PersistentFlags().MarkHidden("upstream-ca")
	_ = cmd.PersistentFlags().MarkHidden("upstream-cert")
	_ = cmd.PersistentFlags().MarkHidden("upstream-key")
}

// strictDecodeConfig do strictDecodeFile check and only verify the rules for now.
func (o *changefeedCommonOptions) strictDecodeConfig(component string, cfg *config.ReplicaConfig) error {
	err := util.StrictDecodeFile(o.configFile, component, cfg)
	if err != nil {
		return err
	}

	_, err = filter.VerifyTableRules(cfg.Filter)

	return err
}

// createChangefeedOptions defines common flags for the `cli changefeed crate` command.
type createChangefeedOptions struct {
	commonChangefeedOptions *changefeedCommonOptions
	apiClient               apiv2client.APIV2Interface

	changefeedID            string
	namespace               string
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
	o.commonChangefeedOptions.addFlags(cmd)
	cmd.PersistentFlags().StringVarP(&o.namespace, "namespace", "n", "default", "Replication task (changefeed) Namespace")
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmd.PersistentFlags().BoolVarP(&o.disableGCSafePointCheck, "disable-gc-check", "", false, "Disable GC safe point check")
	cmd.PersistentFlags().Uint64Var(&o.startTs, "start-ts", 0, "Start ts of changefeed")
	cmd.PersistentFlags().StringVar(&o.timezone, "tz", "SYSTEM", "timezone used when checking sink uri (changefeed timezone is determined by cdc server)")
	// we don't support specify these flags below when cdc version >= 6.2.0
	_ = cmd.PersistentFlags().MarkHidden("tz")
}

// complete adapts from the command line args to the data and client required.
func (o *createChangefeedOptions) complete(f factory.Factory) error {
	client, err := f.APIV2Client()
	if err != nil {
		return err
	}
	o.apiClient = client
	return o.completeReplicaCfg()
}

// completeCfg complete the replica config from file and cmd flags.
func (o *createChangefeedOptions) completeReplicaCfg() error {
	cfg := config.GetDefaultReplicaConfig()
	if len(o.commonChangefeedOptions.configFile) > 0 {
		if err := o.commonChangefeedOptions.strictDecodeConfig("TiCDC changefeed", cfg); err != nil {
			return err
		}
	}

	uri, err := url.Parse(o.commonChangefeedOptions.sinkURI)
	if err != nil {
		return err
	}

	err = cfg.ValidateAndAdjust(uri)
	if err != nil {
		return err
	}

	if o.commonChangefeedOptions.schemaRegistry != "" {
		cfg.Sink.SchemaRegistry = putil.AddressOf(o.commonChangefeedOptions.schemaRegistry)
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

	if o.disableGCSafePointCheck {
		cfg.CheckGCSafePoint = false
	}
	// Complete cfg.
	o.cfg = cfg

	return nil
}

// validate checks that the provided attach options are specified.
func (o *createChangefeedOptions) validate(cmd *cobra.Command) error {
	if o.timezone != "SYSTEM" {
		cmd.Printf(color.HiYellowString("[WARN] --tz is deprecated in changefeed settings.\n"))
	}

	// user is not allowed to set sort-dir at changefeed level
	if o.commonChangefeedOptions.sortDir != "" {
		cmd.Printf(color.HiYellowString("[WARN] --sort-dir is deprecated in changefeed settings. " +
			"Please use `cdc server --data-dir` to start the cdc server if possible, sort-dir will be set automatically. " +
			"The --sort-dir here will be no-op\n"))
		return errors.New("creating changefeed with `--sort-dir`, it's invalid")
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

	return nil
}

func (o *createChangefeedOptions) getChangefeedConfig() *v2.ChangefeedConfig {
	replicaConfig := v2.ToAPIReplicaConfig(o.cfg)
	upstreamConfig := o.getUpstreamConfig()
	return &v2.ChangefeedConfig{
		ID:            o.changefeedID,
		Namespace:     o.namespace,
		StartTs:       o.startTs,
		TargetTs:      o.commonChangefeedOptions.targetTs,
		SinkURI:       o.commonChangefeedOptions.sinkURI,
		ReplicaConfig: replicaConfig,
		PDConfig:      upstreamConfig.PDConfig,
	}
}

func (o *createChangefeedOptions) getUpstreamConfig() *v2.UpstreamConfig {
	var (
		pdAddrs  []string
		caPath   string
		keyPath  string
		certPath string
	)
	if o.commonChangefeedOptions.upstreamPDAddrs != "" {
		pdAddrs = strings.Split(o.commonChangefeedOptions.upstreamPDAddrs, ",")
		caPath = o.commonChangefeedOptions.upstreamCaPath
		certPath = o.commonChangefeedOptions.upstreamCertPath
		keyPath = o.commonChangefeedOptions.upstreamKeyPath
	}
	return &v2.UpstreamConfig{
		PDConfig: v2.PDConfig{
			PDAddrs:       pdAddrs,
			CAPath:        caPath,
			CertPath:      certPath,
			KeyPath:       keyPath,
			CertAllowedCN: nil,
		},
	}
}

// run the `cli changefeed create` command.
func (o *createChangefeedOptions) run(ctx context.Context, cmd *cobra.Command) error {
	tso, err := o.apiClient.Tso().Query(ctx, o.getUpstreamConfig())
	if err != nil {
		return err
	}

	if o.startTs == 0 {
		o.startTs = oracle.ComposeTS(tso.Timestamp, tso.LogicTime)
	}

	if !o.commonChangefeedOptions.noConfirm {
		if err = confirmLargeDataGap(cmd, tso.Timestamp, o.startTs, "create"); err != nil {
			return err
		}
	}

	createChangefeedCfg := o.getChangefeedConfig()

	verifyTableConfig := &v2.VerifyTableConfig{
		PDConfig: v2.PDConfig{
			PDAddrs:       createChangefeedCfg.PDAddrs,
			CAPath:        createChangefeedCfg.CAPath,
			CertPath:      createChangefeedCfg.CertPath,
			KeyPath:       createChangefeedCfg.KeyPath,
			CertAllowedCN: createChangefeedCfg.CertAllowedCN,
		},
		ReplicaConfig: createChangefeedCfg.ReplicaConfig,
		StartTs:       createChangefeedCfg.StartTs,
		SinkURI:       createChangefeedCfg.SinkURI,
	}

	tables, err := o.apiClient.Changefeeds().VerifyTable(ctx, verifyTableConfig)
	if err != nil {
		if strings.Contains(err.Error(), "ErrInvalidIgnoreEventType") {
			supportedEventTypes := filter.SupportedEventTypes()
			eventTypesStr := make([]string, 0, len(supportedEventTypes))
			for _, eventType := range supportedEventTypes {
				eventTypesStr = append(eventTypesStr, string(eventType))
			}
			cmd.Println(fmt.Sprintf("Invalid input, 'ignore-event' parameters can only accept [%s]",
				strings.Join(eventTypesStr, ", ")))
		}
		return err
	}

	ignoreIneligibleTables := false
	if len(tables.IneligibleTables) != 0 {
		if o.cfg.ForceReplicate {
			cmd.Printf("[WARN] Force to replicate some ineligible tables, "+
				"these tables do not have a primary key or a not-null unique key: %#v\n"+
				"[WARN] This may cause data redundancy, "+
				"please refer to the official documentation for details.\n",
				tables.IneligibleTables)
		} else {
			cmd.Printf("[WARN] Some tables are not eligible to replicate, "+
				"because they do not have a primary key or a not-null unique key: %#v\n",
				tables.IneligibleTables)
			if !o.commonChangefeedOptions.noConfirm {
				ignoreIneligibleTables, err = confirmIgnoreIneligibleTables(cmd)
				if err != nil {
					return err
				}
			}
		}
	}

	if o.commonChangefeedOptions.noConfirm {
		ignoreIneligibleTables = true
	}

	createChangefeedCfg.ReplicaConfig.IgnoreIneligibleTable = ignoreIneligibleTables

	info, err := o.apiClient.Changefeeds().Create(ctx, createChangefeedCfg)
	if err != nil {
		if strings.Contains(err.Error(), "ErrInvalidIgnoreEventType") {
			supportedEventTypes := filter.SupportedEventTypes()
			eventTypesStr := make([]string, 0, len(supportedEventTypes))
			for _, eventType := range supportedEventTypes {
				eventTypesStr = append(eventTypesStr, string(eventType))
			}
			cmd.Println(fmt.Sprintf("Invalid input, 'ignore-event' parameters can only accept [%s]",
				strings.Join(eventTypesStr, ", ")))
		}
		return err
	}
	infoStr, err := info.Marshal()
	if err != nil {
		return err
	}
	cmd.Printf("Create changefeed successfully!\nID: %s\nInfo: %s\n", info.ID, infoStr)
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
		Run: func(cmd *cobra.Command, args []string) {
			ctx := cmdcontext.GetDefaultContext()

			util.CheckErr(o.complete(f))
			util.CheckErr(o.validate(cmd))
			util.CheckErr(o.run(ctx, cmd))
		},
	}

	o.addFlags(command)

	return command
}
