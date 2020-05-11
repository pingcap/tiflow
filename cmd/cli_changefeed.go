package cmd

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/spf13/cobra"
)

func newAdminChangefeedCommand() []*cobra.Command {
	cmds := []*cobra.Command{
		{
			Use:   "pause",
			Short: "Pause a replicaiton task (changefeed)",
			RunE: func(cmd *cobra.Command, args []string) error {
				ctx := context.Background()
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
				ctx := context.Background()
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
				ctx := context.Background()
				job := model.AdminJob{
					CfID: changefeedID,
					Type: model.AdminRemove,
				}
				return applyAdminChangefeed(ctx, job)
			},
		},
	}

	for _, cmd := range cmds {
		cmd.PersistentFlags().StringVar(&changefeedID, "changefeed-id", "", "Replication task (changefeed) ID")
		_ = cmd.MarkPersistentFlagRequired("changefeed-id")
	}
	return cmds
}
