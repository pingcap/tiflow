package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"go.uber.org/zap"
)

func main() {
	changefeed := model.DefaultChangeFeedID("test")
	option := kafka.NewOptions()
	option.BrokerEndpoints = []string{"127.0.0.1:9092"}
	option.ClientID = "kafka-client"

	//factory, err := v2.NewFactory(option, changefeed)
	factory, err := kafka.NewSaramaFactory(option, changefeed)
	if err != nil {
		log.Error("create kafka factory failed", zap.Error(err))
		return
	}

	asyncProducer, err := factory.AsyncProducer(make(chan struct{}), make(chan error, 1))
	if err != nil {
		log.Error("create kafka async producer failed", zap.Error(err))
		return
	}
	defer asyncProducer.Close()

	var key []byte
	value := make([]byte, 10240)
	topic := "kafka-go-test"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var total uint64
	var ackTotal uint64

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			rand.Read(value)
			err := asyncProducer.AsyncSend(ctx, topic, 0, key, value, func() {
				atomic.AddUint64(&ackTotal, 1)
			})
			if err != nil {
				log.Error("send kafka message failed", zap.Error(err))
				return
			} else {
				atomic.AddUint64(&total, 1)
			}
		}
	}()

	wg.Add(1)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer func() {
			ticker.Stop()
			defer wg.Done()
		}()
		old := uint64(0)
		oldAck := uint64(0)
		for {
			select {
			case <-ticker.C:
				temp := atomic.LoadUint64(&total)
				qps := (float64(temp) - float64(old)) / 5.0
				old = temp
				temp = atomic.LoadUint64(&ackTotal)
				ackQps := (float64(temp) - float64(oldAck)) / 5.0
				fmt.Printf("total %d, qps is %f, ack qps is %f\n", total, qps, ackQps)
				oldAck = temp
			}
		}
	}()

	if err := asyncProducer.AsyncRunCallback(ctx); err != nil {
		log.Error("run kafka async producer failed", zap.Error(err))
	}
}
