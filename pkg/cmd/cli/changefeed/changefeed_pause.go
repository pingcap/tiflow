package changefeed

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cmd/context"
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewCmdPauseChangefeed(f util.Factory, commonOptions *CommonOptions) *cobra.Command {
	command := &cobra.Command{
		Use:   "pause",
		Short: "Pause a replication task (changefeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.GetDefaultContext()

			job := model.AdminJob{
				CfID: commonOptions.changefeedID,
				Type: model.AdminStop,
			}
			return util.ApplyAdminChangefeed(f, ctx, job, f.GetCredential())
		},
	}

	return command
}
