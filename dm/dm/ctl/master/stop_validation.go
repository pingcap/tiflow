package master

import (
	"context"
	"errors"
	"os"

	"github.com/spf13/cobra"

	"github.com/pingcap/tiflow/dm/dm/ctl/common"
	"github.com/pingcap/tiflow/dm/dm/pb"
)

func NewStopValidationCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validation stop [-s source...] [--all-task] <task-name>",
		Short: "stop validation task",
		RunE: stopValidationFunc,
	}
	cmd.Flags().BoolP("all-task", "", false, "whether stop all task")
	return cmd
}

func stopValidationFunc(cmd *cobra.Command, _ []string) (err error) {
	var (
		sources   []string
		isAllTask bool
		taskName  string
	)
	sources, err = common.GetSourceArgs(cmd)
	if err != nil {
		return err
	}
	isAllTask, err = cmd.Flags().GetBool("all-task")
	if err != nil {
		return err
	}
	if len(cmd.Flags().Args()) > 0 {
		taskName = cmd.Flags().Arg(0)
	}
	// contradiction
	if (len(taskName) > 0 && isAllTask) || (len(taskName) == 0 && !isAllTask) {
		cmd.SetOut(os.Stdout)
		common.PrintCmdUsage(cmd)
		return errors.New("either `task-name` or `all-task` should be set")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resp := pb.StopValidationResponse{}
	err = common.SendRequest(
		ctx,
		"StopValidation",
		&pb.StopValidationRequest{
			IsAllTask: isAllTask,
			TaskName:  taskName,
			Sources:   sources,
		},
		&resp,
	)
	common.PrettyPrintResponse(&resp)
	return nil
}
