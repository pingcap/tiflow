package unsafe

import (
	"github.com/pingcap/ticdc/pkg/cmd/util"
	"github.com/spf13/cobra"
)

type CommonOptions struct {
	noConfirm bool
}

func NewCommonOptions() *CommonOptions {
	return &CommonOptions{
		noConfirm: false,
	}
}

func NewCmdUnsafe(f util.Factory) *cobra.Command {
	commonOptions := NewCommonOptions()

	command := &cobra.Command{
		Use:    "unsafe",
		Hidden: true,
	}

	command.AddCommand(NewCmdReset(f, commonOptions))
	command.AddCommand(NewCmdShowMetadata(f))
	command.AddCommand(NewCmdDeleteServiceGcSafepoint(f, commonOptions))

	command.PersistentFlags().BoolVar(&commonOptions.noConfirm, "no-confirm", false, "Don't ask user whether to confirm executing meta command")

	return command
}
