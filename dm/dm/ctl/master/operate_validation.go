package master

import (
	"context"
	"errors"
	"os"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

func NewIgnoreValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ignore-error <task-name> <error-id|--all>",
		Short: "operate validation error",
		RunE:  ignoreError,
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func NewResolveValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "make-resolve <task-name> <error-id|--all>",
		Short: "operate validation error",
		RunE:  resolveError,
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func NewClearValidationErrorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "clear <task-name> <error-id|--all>",
		Short: "operate validation error",
		RunE:  clearError,
	}
	cmd.Flags().Bool("all", false, "all task")
	return cmd
}

func getFlags(cmd *cobra.Command) (taskName string, errId int, isAll bool, err error) {
	if len(cmd.Flags().Args()) < 1 {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return "", -1, false, errors.New("task name should be specified")
	}
	var errIdStr string
	taskName = cmd.Flags().Arg(0)
	if len(cmd.Flags().Args()) > 1 {
		errIdStr = cmd.Flags().Arg(1)
		errId, err = strconv.Atoi(errIdStr)
		if err != nil {
			return "", -1, false, errors.New("error-id not valid")
		}
	} else {
		errId = -1
	}
	isAll, err = cmd.Flags().GetBool("all")
	if err != nil {
		return "", -1, false, err
	}
	if (errId < 0 && !isAll) || (errId > 0 && isAll) {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return "", -1, false, errors.New("either `--all` or `error-id` should be set")
	}
	return taskName, errId, isAll, nil
}

func operateError(taskName string, errId int, isAll bool, op string) (resp *pb.OperateValidationErrorResponse, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resp = &pb.OperateValidationErrorResponse{}
	err = common.SendRequest(
		ctx,
		"OperateValidationError",
		&pb.OperateValidationErrorRequest{
			Op: op,
			TaskName: taskName,
			Id: int32(errId),
			IsAllError: isAll,
		},
		&resp,
	)
	return resp, err
}

func ignoreError(cmd *cobra.Command, _ []string) (err error) {
	var (
		taskName string
		errId    int
		isAll    bool
		resp *pb.OperateValidationErrorResponse
	)
	taskName, errId, isAll, err = getFlags(cmd)
	if err != nil {
		return err
	}
	resp, err = operateError(taskName, errId, isAll, "ignore")
	common.PrettyPrintResponse(resp)
	return nil
}

func resolveError(cmd *cobra.Command, _ []string) (err error) {
	var (
		taskName string
		errId    int
		isAll    bool
		resp *pb.OperateValidationErrorResponse
	)
	taskName, errId, isAll, err = getFlags(cmd)
	if err != nil {
		return err
	}
	resp, err = operateError(taskName, errId, isAll, "resolve")
	common.PrettyPrintResponse(resp)
	return nil
}

func clearError(cmd *cobra.Command, _ []string) (err error) {
	var (
		taskName string
		errId    int
		isAll    bool
		resp *pb.OperateValidationErrorResponse
	)
	taskName, errId, isAll, err = getFlags(cmd)
	if err != nil {
		return err
	}
	resp, err = operateError(taskName, errId, isAll, "clear")
	common.PrettyPrintResponse(resp)
	return nil
}
