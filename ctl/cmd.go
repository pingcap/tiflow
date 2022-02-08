package ctl

import (
	"context"
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func NewRunFake() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run-fake [--executor-addr addr] [--executor-id id]",
		Short: "Run a fake workload to a specific executor",
		RunE:  runFakeFunc,
	}
	cmd.Flags().StringP("executor-addr", "", "", "the targeted executor address")
	cmd.Flags().StringP("executor-id", "", "", "the targeted executor id")
	return cmd
}

func runFakeFunc(cmd *cobra.Command, _ []string) error {
	execAddr, err := cmd.Flags().GetString("executor-addr")
	if err != nil {
		fmt.Print("error in parse `--executor-addr`")
		return err
	}
	execID, err := cmd.Flags().GetString("executor-id")
	if err != nil {
		fmt.Print("error in parse `--executor-id`")
		return err
	}
	err = cltManager.AddExecutor(model.ExecutorID(execID), execAddr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	resp, err := cltManager.ExecutorClient(model.ExecutorID(execID)).Send(ctx, &client.ExecutorRequest{
		Cmd: client.CmdDispatchTask,
		Req: &pb.DispatchTaskRequest{
			TaskTypeId: int64(lib.CvsJobMaster),
			TaskConfig: []byte(`{"srcHost":"127.0.0.1:1234","srcDir":"data","dstHost":"127.0.0.1:1234","dstDir":"data2"}`),
			MasterId:   uuid.New().String(), //  use a unique ID to force Init the master each time,
			WorkerId:   uuid.New().String(),
		},
	})
	if err != nil {
		log.L().Error("failed to dispatch master", zap.Error(err))
		os.Exit(1)
	}
	log.L().Info("resp", zap.Any("resp", resp))
	return nil
}
