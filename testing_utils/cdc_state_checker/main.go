package main

import (
	"context"
	"flag"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	pd = flag.String("pd", "http://127.0.0.1:2379", "PD address and port")
)

func main() {
	cdcMonitor, err := newCDCMonitor(context.TODO(), *pd)

	if err != nil {
		log.Panic("Error creating CDCMonitor", zap.Error(err))
	}

	err = cdcMonitor.run(context.TODO())
	log.Info("cdcMonitor exited", zap.Error(err))
}
