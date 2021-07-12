package changefeed

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

type removeChangefeedOptions struct {
	optForceRemove bool
}

// newRemoveChangefeedOptions creates new options for the `cli changefeed remove` command.
func newRemoveChangefeedOptions() *removeChangefeedOptions {
	return &removeChangefeedOptions{}
}

// newCmdRemoveChangefeed creates the `cli changefeed remove` command.
func newCmdRemoveChangefeed(f util.Factory, commonOptions *commonOptions) *cobra.Command {
	o := newRemoveChangefeedOptions()

	command := &cobra.Command{
		Use:   "remove",
		Short: "Remove a replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			job := model.AdminJob{
				CfID: commonOptions.changefeedID,
				Type: model.AdminRemove,
				Opts: &model.AdminJobOption{
					ForceRemove: o.optForceRemove,
				},
			}

			ctx := context.GetDefaultContext()
			etcdClient, err := f.EtcdClient()
			if err != nil {
				return err
			}

			return applyAdminChangefeed(ctx, etcdClient, job, f.GetCredential())
		},
	}

	command.PersistentFlags().BoolVarP(&o.optForceRemove, "force", "f", false, "remove all information of the changefeed")

	return command
}
