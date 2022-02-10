package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/ctl"
	"github.com/hanfei1991/microcosm/jobmaster/benchmark"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pkg/errors"
)

func main() {
	cmd := os.Args[1]
	ctx := context.Background()
	addr := ""
	err := log.InitLogger(&log.Config{
		Level: "info",
	})
	if err != nil {
		fmt.Printf("err: %v", err)
		os.Exit(1)
	}
	switch cmd {
	case "submit-job", "cancel-job":
		flag1 := os.Args[2]
		if flag1 != "--master-addr" {
			fmt.Printf("no master address found")
			os.Exit(1)
		}
		addr = os.Args[3]
	case "run-fake":
		ctl.MainStart(ctx, os.Args[1:])
		os.Exit(0)
	default:
		fmt.Printf("submit-job --config configFile")
		os.Exit(0)
	}
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
