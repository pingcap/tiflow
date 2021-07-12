package changefeed

import (
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/r3labs/diff"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"strings"
)

func NewCmdUpdateChangefeed(f util.Factory, commonOptions *commonOptions) *cobra.Command {
	o := NewCreateCommonOptions()

	command := &cobra.Command{
		Use:   "update",
		Short: "Update config of an existing replication task (changefeed)",
		Long:  ``,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			old, err := etcdClient.GetChangeFeedInfo(ctx, commonOptions.changefeedID)
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
					info.TargetTs = o.targetTs
				case "sink-uri":
					info.SinkURI = o.sinkURI
				case "config":
					cfg := info.Config
					if err = o.validateReplicaConfig("TiCDC changefeed", cfg); err != nil {
						log.Error("decode config file error", zap.Error(err))
					}
				case "opts":
					for _, opt := range o.opts {
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
					info.Engine = o.sortEngine
				case "cyclic-replica-id":
					filter := make([]uint64, 0, len(o.cyclicFilterReplicaIDs))
					for _, id := range o.cyclicFilterReplicaIDs {
						filter = append(filter, uint64(id))
					}
					info.Config.Cyclic.FilterReplicaID = filter
				case "cyclic-sync-ddl":
					info.Config.Cyclic.SyncDDL = o.cyclicSyncDDL
				case "sync-point":
					info.SyncPointEnabled = o.syncPointEnabled
				case "sync-interval":
					info.SyncPointInterval = o.syncPointInterval
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

			resp, err := applyOwnerChangefeedQuery(ctx, etcdClient, commonOptions.changefeedID, f.GetCredential())
			// if no cdc owner exists, allow user to update changefeed config
			if err != nil && errors.Cause(err) != util.ErrOwnerNotFound {
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

			if !commonOptions.NoConfirm {
				cmd.Printf("Could you agree to apply changes above to changefeed [Y/N]\n")
				var yOrN string
				_, err = fmt.Scan(&yOrN)
				if err != nil {
					return err
				}
				if strings.ToLower(strings.TrimSpace(yOrN)) != "y" {
					cmd.Printf("No update to changefeed.\n")
					return nil
				}
			}

			err = etcdClient.SaveChangeFeedInfo(ctx, info, commonOptions.changefeedID)
			if err != nil {
				return err
			}
			infoStr, err := info.Marshal()
			if err != nil {
				return err
			}
			cmd.Printf("Update changefeed config successfully! "+
				"Will take effect only if the changefeed has been paused before this command"+
				"\nID: %s\nInfo: %s\n", commonOptions.changefeedID, infoStr)
			return nil
		},
	}

	o.AddFlags(command.PersistentFlags())
	_ = command.MarkPersistentFlagRequired("changefeed-id")

	return command
}
