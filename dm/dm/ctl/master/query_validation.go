package master

import (
	"context"
	"errors"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
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
	isIgnoredError, err = cmd.Flags().GetBool("ignored-error")
	if err != nil {
		return err
	}
	if isAllError && isIgnoredError {
		// conflict
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("flag `all-error` and `ignored-error` are mutually exclusive")
	}
	// TODO: handle the contradiction between `isAllError` and `isIgnoredError`
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.GetValidationErrorResponse{}
	err = common.SendRequest(
		ctx,
		"GetValidationError",
		&pb.GetValidationErrorRequest{
			IsIgnoredError: isIgnoredError,
			IsAllError:     isAllError,
			TaskName:       taskName,
		},
		&resp,
	)
	common.PrettyPrintResponse(resp)
	return nil
}

func NewQueryValidationStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status <task-name>",
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := &pb.GetValidationStatusResponse{}
	err = common.SendRequest(
		ctx,
		"GetValidationStatus",
		&pb.GetValidationStatusRequest{
			TaskName:     taskName,
			FilterStatus: status,
		},
		&resp,
	)
	common.PrettyPrintResponse(resp)
	return nil
}
