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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cmdcontext "github.com/pingcap/tiflow/pkg/cmd/context"
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/etcd"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/r3labs/diff"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

// updateChangefeedOptions defines common flags for the `cli changefeed update` command.
type updateChangefeedOptions struct {
	etcdClient *etcd.CDCEtcdClient

	credential *security.Credential

	commonChangefeedOptions *changefeedCommonOptions
	changefeedID            string
}

// newUpdateChangefeedOptions creates new options for the `cli changefeed update` command.
func newUpdateChangefeedOptions(commonChangefeedOptions *changefeedCommonOptions) *updateChangefeedOptions {
	return &updateChangefeedOptions{
		commonChangefeedOptions: commonChangefeedOptions,
	}
}

// addFlags receives a *cobra.Command reference and binds
// flags related to template printing to it.
func (o *updateChangefeedOptions) addFlags(cmd *cobra.Command) {
	if o == nil {
		return
	}

	o.commonChangefeedOptions.addFlags(cmd)
	cmd.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	_ = cmd.MarkPersistentFlagRequired("changefeed-id")
}

// complete adapts from the command line args to the data and client required.
func (o *updateChangefeedOptions) complete(f factory.Factory) error {
	etcdClient, err := f.EtcdClient()
	if err != nil {
		return err
	}

	o.etcdClient = etcdClient

	o.credential = f.GetCredential()

	return nil
}

// run the `cli changefeed update` command.
func (o *updateChangefeedOptions) run(cmd *cobra.Command) error {
	ctx := cmdcontext.GetDefaultContext()

	resp, err := sendOwnerChangefeedQuery(ctx, o.etcdClient,
		model.DefaultChangeFeedID(o.changefeedID),
		o.credential)
	// if no cdc owner exists, allow user to update changefeed config
	if err != nil && errors.Cause(err) != cerror.ErrOwnerNotFound {
		return err
	}
	// Note that the correctness of the logic here depends on the return value of `/capture/owner/changefeed/query` interface.
	// TODO: Using error codes instead of string containing judgments
	if err == nil && !strings.Contains(resp, `"state": "stopped"`) {
		return errors.Errorf("can only update changefeed config when it is stopped\nstatus: %s", resp)
	}

	old, err := o.etcdClient.GetChangeFeedInfo(ctx,
		model.DefaultChangeFeedID(o.changefeedID))
	if err != nil {
		return err
	}

	newInfo, err := o.applyChanges(old, cmd)
	if err != nil {
		return err
	}

	changelog, err := diff.Diff(old, newInfo)
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

	if !o.commonChangefeedOptions.noConfirm {
		cmd.Printf("Could you agree to apply changes above to changefeed [Y/N]\n")
		confirmed := readInput(cmd)
		if !confirmed {
			cmd.Printf("No update to changefeed.\n")
			return nil
		}
	}

	err = o.etcdClient.SaveChangeFeedInfo(ctx, newInfo,
		model.DefaultChangeFeedID(o.changefeedID))
	if err != nil {
		return err
	}
	infoStr, err := newInfo.Marshal()
	if err != nil {
		return err
	}
	cmd.Printf("Update changefeed config successfully! "+
		"\nID: %s\nInfo: %s\n", o.changefeedID, infoStr)

	return nil
}

// applyChanges applies the new changes to the old changefeed.
func (o *updateChangefeedOptions) applyChanges(oldInfo *model.ChangeFeedInfo, cmd *cobra.Command) (*model.ChangeFeedInfo, error) {
	newInfo, err := oldInfo.Clone()
	if err != nil {
		return nil, err
	}

	cmd.Flags().Visit(func(flag *pflag.Flag) {
		switch flag.Name {
		case "target-ts":
			newInfo.TargetTs = o.commonChangefeedOptions.targetTs
		case "sink-uri":
			newInfo.SinkURI = o.commonChangefeedOptions.sinkURI
		case "config":
			cfg := newInfo.Config
			if err = o.commonChangefeedOptions.strictDecodeConfig("TiCDC changefeed", cfg); err != nil {
				log.Error("decode config file error", zap.Error(err))
			}
		case "schema-registry":
			newInfo.Config.Sink.SchemaRegistry = o.commonChangefeedOptions.schemaRegistry
		case "opts":
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
				newInfo.Opts[key] = value
			}

		case "sort-engine":
			newInfo.Engine = o.commonChangefeedOptions.sortEngine
		case "cyclic-replica-id":
			filter := make([]uint64, 0, len(o.commonChangefeedOptions.cyclicFilterReplicaIDs))
			for _, id := range o.commonChangefeedOptions.cyclicFilterReplicaIDs {
				filter = append(filter, uint64(id))
			}
			newInfo.Config.Cyclic.FilterReplicaID = filter
		case "cyclic-sync-ddl":
			newInfo.Config.Cyclic.SyncDDL = o.commonChangefeedOptions.cyclicSyncDDL
		case "sync-point":
			newInfo.SyncPointEnabled = o.commonChangefeedOptions.syncPointEnabled
		case "sync-interval":
			newInfo.SyncPointInterval = o.commonChangefeedOptions.syncPointInterval
		case "sort-dir":
			log.Warn("this flag cannot be updated and will be ignored", zap.String("flagName", flag.Name))
		case "changefeed-id", "no-confirm", "cyclic-filter-replica-ids":
			// Do nothing, these are some flags from the changefeed command,
			// we don't use it to update, but we do use these flags.
		case "interact":
			// Do nothing, this is a flags from the cli command
			// we don't use it to update.
		case "pd", "log-level", "key", "cert", "ca":
			// Do nothing, this is a flags from the cli command
			// we don't use it to update, but we do use these flags.
		default:
			// use this default branch to prevent new added parameter is not added
			log.Warn("unsupported flag, please report a bug", zap.String("flagName", flag.Name))
		}
	})
	if err != nil {
		return nil, err
	}

	return newInfo, nil
}

// newCmdPauseChangefeed creates the `cli changefeed update` command.
func newCmdUpdateChangefeed(f factory.Factory) *cobra.Command {
	commonChangefeedOptions := newChangefeedCommonOptions()
	o := newUpdateChangefeedOptions(commonChangefeedOptions)

	command := &cobra.Command{
		Use:   "update",
		Short: "Update config of an existing replication task (changefeed)",
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
