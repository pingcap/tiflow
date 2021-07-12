package tso

import (
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewCmdTso(f util.Factory) *cobra.Command {
	command := &cobra.Command{
		Use:   "tso",
		Short: "Manage tso",
	}
	command.AddCommand(NewCmdQueryTso(f))

	return command
}
