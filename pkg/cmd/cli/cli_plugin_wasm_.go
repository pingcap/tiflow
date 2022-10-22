package cli

import (
	"github.com/pingcap/tiflow/pkg/cmd/factory"
	"github.com/spf13/cobra"
)

func newCmdPluginWasm(f factory.Factory) *cobra.Command {
	cmds := &cobra.Command{
		Use:   "wasm",
		Short: "Manage Wasm plugin",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
	cmds.AddCommand(
		newCmdPluginWasmUpload(f),
		newCmdPluginWasmList(f),
	)
	return cmds
}
