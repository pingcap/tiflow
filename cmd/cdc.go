package cmd

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-cdc/cdc"
)

func feed() {
	detail := cdc.ChangeFeedDetail{
		SinkURI:      "root@tcp(127.0.0.1:3306)/test",
		Opts:         make(map[string]string),
		CheckpointTS: 0,
		CreateTime:   time.Now(),
	}

	feed, err := cdc.NewSubChangeFeed([]string{"localhost:2379"}, detail)
	if err != nil {
		log.Error("NewChangeFeed failed", zap.Error(err))
		return
	}

	err = feed.Start(context.Background())
	if err != nil {
		log.Error("feed failed", zap.Error(err))
	}
}
