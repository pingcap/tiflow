package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/google/uuid"
	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/jobmaster/benchmark"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func main() {
	cmd := os.Args[1]
	addr := ""
	switch cmd {
	case "submit-job", "cancel-job":
		flag1 := os.Args[2]
		if flag1 != "--master-addr" {
			fmt.Printf("no master address found")
			os.Exit(1)
		}
		addr = os.Args[3]
	case "run-fake":
		flag1 := os.Args[2]
		if flag1 != "--executor-addr" {
			fmt.Printf("no executor address found")
			os.Exit(1)
		}
		addr = os.Args[3]

		flag3 := os.Args[4]
		if flag3 != "--executor-id" {
			fmt.Printf("executor ID is not found")
			os.Exit(1)
		}
		nodeID := os.Args[5]

		cliManager := client.NewClientManager()
		err := cliManager.AddExecutor(model.ExecutorID(nodeID), addr)
		if err != nil {
			log.L().Error("failed to create executor client", zap.Error(err))
			os.Exit(1)
		}

		for i := 0; i < 1; i++ {
			resp, err := cliManager.ExecutorClient(model.ExecutorID(nodeID)).Send(context.TODO(), &client.ExecutorRequest{
				Cmd: client.CmdDispatchTask,
				Req: &pb.DispatchTaskRequest{
					TaskTypeId: int64(lib.CvsJobMaster),
					TaskConfig: []byte(`{"srcHost":"127.0.0.1:1234","srcDir":"data","dstHost":"127.0.0.1:1234","dstDir":"data1"}`),
					MasterId:   uuid.New().String(), //  use a unique ID to force Init the master each time,
					WorkerId:   uuid.New().String(),
				},
			})
			if err != nil {
				log.L().Error("failed to dispatch master", zap.Error(err))
				os.Exit(1)
			}
			log.L().Info("resp", zap.Any("resp", resp))
		}
	default:
		fmt.Printf("submit-job --config configFile")
		os.Exit(0)
	}
	ctx := context.Background()
	clt, err := client.NewMasterClient(ctx, []string{addr})
	if err != nil {
		fmt.Printf("err: %v", err)
		os.Exit(1)
	}

	if cmd == "submit-job" {
		args := os.Args[4:]
		cfg := benchmark.NewConfig()
		err = cfg.Parse(args)
		switch errors.Cause(err) {
		case nil:
		case flag.ErrHelp:
			os.Exit(0)
		default:
			fmt.Printf("err1: %v", err)
			os.Exit(2)
		}

		configJSON, err := json.Marshal(cfg)
		if err != nil {
			fmt.Printf("err2: %v", err)
		}

		req := &pb.SubmitJobRequest{
			Tp:     pb.JobType_Benchmark,
			Config: configJSON,
			User:   "hanfei",
		}
		resp, err := clt.SubmitJob(context.Background(), req)
		if err != nil {
			fmt.Printf("err: %v", err)
			return
		}
		if resp.Err != nil {
			fmt.Printf("err: %v", resp.Err.Message)
			return
		}
		fmt.Printf("submit job successful JobID:%d JobIDStr:%s\n", resp.JobId, resp.JobIdStr)
	}
	if cmd == "cancel-job" {
		flag1 := os.Args[4]
		jobID, err := strconv.ParseInt(flag1, 10, 32)
		if err != nil {
			fmt.Print(err.Error())
			os.Exit(1)
		}
		req := &pb.CancelJobRequest{
			JobId: int32(jobID),
		}
		resp, err := clt.CancelJob(context.Background(), req)
		if err != nil {
			fmt.Printf("err: %v", err)
			return
		}
		if resp.Err != nil {
			fmt.Printf("err: %v", resp.Err.Message)
			return
		}
		fmt.Print("cancel job successful")
	}
}
