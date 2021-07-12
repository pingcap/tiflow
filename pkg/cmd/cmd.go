package cmd

import (
	"github.com/pingcap/ticdc/pkg/cmd/server"
	"github.com/spf13/cobra"
	"os"
)

func NewCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "cdc",
		Short: "CDC",
		Long:  `Change Data Capture`,
	}
}

// Execute runs the root command
func Execute() {
	cmd := NewCmd()
	// Outputs cmd.Print to stdout.
	cmd.SetOut(os.Stdout)
	cmd.AddCommand(server.NewCmdServer())
	if err := cmd.Execute(); err != nil {
		cmd.Println(err)
		os.Exit(1)
	}
}
