package cli

import (
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/spf13/cobra"
)

func newCmdPlugin(f factory.Factory) *cobra.Command {
	cmds := &cobra.Command{
		Use:   "plugin",
		Short: "Manage plugin (Currently support Wasm, Go and HTTP plugins)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
	cmds.AddCommand(
		newCmdPluginWasm(f),
	)
	return cmds
}
