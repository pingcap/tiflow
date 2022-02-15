package main

import (
	"context"
	"fmt"
	"os"

	"github.com/hanfei1991/microcosm/ctl"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

func main() {
	ctx := context.Background()
	err := log.InitLogger(&log.Config{
		Level: "info",
	})
	if err != nil {
		fmt.Printf("err: %v", err)
		os.Exit(1)
	}
	ctl.MainStart(ctx, os.Args[1:])
}
