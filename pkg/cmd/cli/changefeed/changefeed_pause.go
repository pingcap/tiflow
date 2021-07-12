package changefeed

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

// newCmdPauseChangefeed creates the `cli changefeed pause` command.
func newCmdPauseChangefeed(f util.Factory, commonOptions *commonOptions) *cobra.Command {
	command := &cobra.Command{
		Use:   "pause",
		Short: "Pause a replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			job := model.AdminJob{
				CfID: commonOptions.changefeedID,
				Type: model.AdminStop,
			}

			ctx := context.GetDefaultContext()

			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			return applyAdminChangefeed(ctx, etcdClient, job, f.GetCredential())
		},
	}

	return command
}
