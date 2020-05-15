package cmd

import (
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/spf13/cobra"
)

func newCaptureCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "capture",
		Short: "Manage capture (capture is a CDC server instance)",
	}
	command.AddCommand(
		newListCaptureCommand(),
		// TODO: add resign owner command
	)
	return command
}

func newListCaptureCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "list",
		Short: "List all captures in TiCDC cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := defaultContext
			captures, err := getAllCaptures(ctx)
			if err != nil {
				return err
			}
			return jsonPrint(cmd, captures)
		},
	}
	return command
}
