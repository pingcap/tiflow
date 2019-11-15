package cmd

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/model"
)

func feed() {
	detail := model.ChangeFeedDetail{
		SinkURI:    "root@tcp(127.0.0.1:3306)/test",
		Opts:       make(map[string]string),
		CreateTime: time.Now(),
	}

	processor, err := cdc.NewProcessor([]string{"http://localhost:2379"}, detail, "test-changefeed", "test-capture")
	if err != nil {
		log.Error("NewProcessor failed", zap.Error(err))
		return
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		processor.Run(context.Background(), errCh)
	}()
	wg.Wait()
	select {
	case err := <-errCh:
		log.Error("feed failed", zap.Error(err))
	default:
	}
}
