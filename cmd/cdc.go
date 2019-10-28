package cmd

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/tidb-cdc/cdc"
)

func feed() {
	detail := cdc.ChangeFeedDetail{
		SinkURI:    "root@tcp(127.0.0.1:3306)/test",
		Opts:       make(map[string]string),
		CreateTime: time.Now(),
	}

	feed, err := cdc.NewSubChangeFeed([]string{"http://localhost:2379"}, detail)
	if err != nil {
		log.Error("NewChangeFeed failed", zap.Error(err))
		return
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		feed.Start(context.Background(), errCh)
	}()
	wg.Wait()
	select {
	case err := <-errCh:
		log.Error("feed failed", zap.Error(err))
	default:
	}
}
