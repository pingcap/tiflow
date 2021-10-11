package main

import (
	"context"
	"flag"
	"log"
	"microcosom/config"
	"microcosom/runtime"
	"os"
	"sync"
	"time"

	"github.com/pingcap/errors"
)

// run benchmark for two kinds of runtime.
func main() {
	// 1 server number
	// 2 table number
	// 3 ...
	// extract configure
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Fatalf("meet fatal err %v", err)
		os.Exit(2)
	}
	log.Printf("cfg table num %d addr %s", cfg.TableNum, cfg.Servers[0])
	s, err := runtime.BuildScheduler(cfg)
	if err != nil {
		log.Fatalf("meet fatal err %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(10)
	for i:=0; i< 10; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Timeout) * time.Second)
			defer cancel()
			err = s.Run(ctx)
			wg.Done()
			if err != nil {
				log.Fatalf("meet fatal err %v", err)
			}
		}()
	}
	wg.Wait()
	s.ShowStats(cfg.Timeout)
}
