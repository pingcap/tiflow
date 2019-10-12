package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-cdc/cdc"
	"github.com/pingcap/tidb-cdc/pkg/util"
)

func feed() {
	detail := cdc.ChangeFeedDetail{
		SinkURI:      "root@tcp(127.0.0.1:3306)/test",
		Opts:         make(map[string]string),
		CheckpointTS: 0,
		CreateTime:   time.Now(),
	}

	err := util.InitLogger(&util.Config{
		File:  "cdc.log",
		Level: "debug",
	})
	if err != nil {
		fmt.Printf("init logger error %v", errors.ErrorStack(err))
		os.Exit(1)
	}

	feed, err := cdc.NewChangeFeed([]string{"localhost:2379"}, detail)
	if err != nil {
		log.Error("NewChangeFeed failed", zap.Error(err))
		return
	}

	err = feed.Start(context.Background())
	if err != nil {
		log.Error("feed failed", zap.Error(err))
	}
}
