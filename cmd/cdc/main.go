package main

import (
	"context"
	"time"

	"github.com/pingcap/tidb-cdc/cdc"
)

func main() {
	detail := cdc.ChangeFeedDetail{
		SinkURI:      "",
		Opts:         make(map[string]string),
		CheckpointTS: 0,
		CreateTime:   time.Now(),
	}

	feed := cdc.NewChangeFeed(detail)

	feed.Start(context.Background())
}
