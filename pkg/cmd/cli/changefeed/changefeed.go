package changefeed

import (
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

type CommonOptions struct {
	changefeedID string
	NoConfirm    bool
}

func NewCommonOptions() *CommonOptions {
	return &CommonOptions{
		changefeedID: "",
		NoConfirm:    false,
	}
}

func NewCmdChangefeed(f util.Factory) *cobra.Command {
	o := NewCommonOptions()
	cmds := &cobra.Command{
		Use:   "changefeed",
		Short: "Manage changefeed (changefeed is a replication task)",
	}

	cmds.AddCommand(NewCmdListChangefeeds(f))
	cmds.AddCommand(NewCmdQueryChangefeed(f, o))
	cmds.AddCommand(NewCmdPauseChangefeed(f, o))
	cmds.AddCommand(NewCmdResumeChangefeed(f, o))
	cmds.AddCommand(NewCmdRemoveChangefeed(f, o))
	cmds.AddCommand(NewCmdCreateChangefeed(f, o))
	cmds.AddCommand(NewCmdUpdateChangefeed(f, o))
	cmds.AddCommand(NewCmdStatisticsChangefeed(f, o))
	cmds.AddCommand(NewCmdCyclicChangefeed(f, o))

	cmds.PersistentFlags().StringVarP(&o.changefeedID, "changefeed-id", "c", "", "Replication task (changefeed) ID")
	cmds.PersistentFlags().BoolVar(&o.NoConfirm, "no-confirm", false, "Don't ask user whether to ignore ineligible table")

	_ = cmds.MarkPersistentFlagRequired("changefeed-id")
	return cmds
}
