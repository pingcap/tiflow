package processor

import (
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

func NewCmdProcessor(f util.Factory) *cobra.Command {
	command := &cobra.Command{
		Use:   "processor",
		Short: "Manage processor (processor is a sub replication task running on a specified capture)",
	}
	command.AddCommand(NewCmdListProcessor(f))
	command.AddCommand(NewCmdQueryProcessor(f))

	return command
}
