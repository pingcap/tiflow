package master

import "github.com/spf13/cobra"

func NewValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validation",
		Short: "operate or query validation task",
	}
	cmd.AddCommand(
		NewStartStopValidationCmd(),
		NewQueryValidationErrorCmd(),
		NewQueryValidationStatusCmd(),
	)
	return cmd
}
