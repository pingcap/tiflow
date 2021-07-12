package changefeed

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

type RemoveChangefeedOptions struct {
	optForceRemove bool
}

func NewRemoveChangefeedOptions() *RemoveChangefeedOptions {
	return &RemoveChangefeedOptions{
		optForceRemove: false,
	}
}

func NewCmdRemoveChangefeed(f util.Factory, commonOptions *commonOptions) *cobra.Command {
	o := NewRemoveChangefeedOptions()

	command := &cobra.Command{
		Use:   "remove",
		Short: "Remove a replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()

			job := model.AdminJob{
				CfID: commonOptions.changefeedID,
				Type: model.AdminRemove,
				Opts: &model.AdminJobOption{
					ForceRemove: o.optForceRemove,
				},
			}

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
