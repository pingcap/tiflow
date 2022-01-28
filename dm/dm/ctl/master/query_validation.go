package master

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

func NewQueryValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show-errors [--ignored-error|--all-error] <task-name>",
		Short: "show error of the validation task",
		RunE:  queryValidationError,
	}
	cmd.Flags().BoolP("all-error", "", false, "show all error")
	cmd.Flags().BoolP("ignored-error", "", false, "show ignored error")
	return cmd
}

func queryValidationError(cmd *cobra.Command, _ []string) (err error) {
	var (
		isAllError     bool
		isIgnoredError bool
		taskName       string
	)
	if len(cmd.Flags().Args()) == 0 {
		return errors.New("task name should be specified")
	}
	taskName = cmd.Flags().Arg(0)
	isAllError, err = cmd.Flags().GetBool("all-error")
	if err != nil {
		return err
	}
	isIgnoredError, err = cmd.Flags().GetBool("ignored-err")
	if err != nil {
		return err
	}
	fmt.Printf("taskName: %s, all-error: %v, ignore-error: %v\n", taskName, isAllError, isIgnoredError)
	return nil
}

func NewQueryValidationStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "query-status <task-name>",
		Short: "query validation status of a task",
		RunE:  queryValidationStatus,
	}
	cmd.Flags().String("status", "Running", "filter status")
	return cmd
}

func queryValidationStatus(cmd *cobra.Command, _ []string) error {
	var (
		status   string
		taskName string
		err      error
	)

	if len(cmd.Flags().Args()) == 0 {
		return errors.New("task name should be specified")
	}
	taskName = cmd.Flags().Arg(0)
	status, err = cmd.Flags().GetString("status")
	if err != nil {
		return err
	}
	fmt.Printf("taskName: %s, status: %s\n", taskName, status)
	return nil
}
