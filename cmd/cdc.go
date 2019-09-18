package cmd

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc"
	"go.uber.org/zap"
)

func feed() {
	detail := cdc.ChangeFeedDetail{
		SinkURI:      "root:123456@tcp(127.0.0.1:3306)/test",
		Opts:         make(map[string]string),
		CheckpointTS: 0,
		CreateTime:   time.Now(),
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
