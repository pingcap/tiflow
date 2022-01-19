// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pipeline

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/pipeline"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/util/testleak"
	"go.uber.org/zap"
)

type mounterNodeSuite struct{}

var _ = check.Suite(&mounterNodeSuite{})

type checkNode struct {
	c        *check.C
	count    int
	expected int
}

func (n *checkNode) Init(ctx pipeline.NodeContext) error {
	// do nothing
	return nil
}

func (n *checkNode) Receive(ctx pipeline.NodeContext) error {
	message := ctx.Message()
	if message.Tp == pipeline.MessageTypePolymorphicEvent {
		if message.PolymorphicEvent.RawKV.OpType == model.OpTypeResolved {
			n.c.Assert(n.count, check.Equals, n.expected)
			return errors.New("finished")
		}
		n.c.Assert(message.PolymorphicEvent.Row, check.NotNil)
	}

	if n.count%100 == 0 {
		log.Info("message received", zap.Int("count", n.count))
	}

	if n.count == basicsTestMessageCount/2 {
		log.Info("sleeping for 5 seconds to simulate blocking")
		time.Sleep(time.Second * 5)
	}
	n.count++
	return nil
}

func (n *checkNode) Destroy(ctx pipeline.NodeContext) error {
	return nil
}

const (
	basicsTestMessageCount = 10000
)

func generateMockRawKV(ts uint64) *model.RawKVEntry {
	return &model.RawKVEntry{
		OpType:   model.OpTypePut,
		Key:      []byte{},
		Value:    []byte{},
		OldValue: nil,
		StartTs:  ts - 5,
		CRTs:     ts,
		RegionID: 0,
	}
}

func (s *mounterNodeSuite) TestMounterNodeBasics(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := cdcContext.WithCancel(cdcContext.NewBackendContext4Test(false))
	defer cancel()

	ctx = cdcContext.WithErrorHandler(ctx, func(err error) error {
		return nil
	})
	runnersSize, outputChannelSize := 2, 64
	p := pipeline.NewPipeline(ctx, 0, runnersSize, outputChannelSize)
	mounterNode := newMounterNode()
	p.AppendNode(ctx, "mounter", mounterNode)

	checkNode := &checkNode{
		c:        c,
		count:    0,
		expected: basicsTestMessageCount,
	}
	p.AppendNode(ctx, "check", checkNode)

	var sentCount int64
	sendMsg := func(p *pipeline.Pipeline, msg pipeline.Message) {
		err := retry.Do(context.Background(), func() error {
			return p.SendToFirstNode(msg)
		}, retry.WithBackoffBaseDelay(10), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(100))
		atomic.AddInt64(&sentCount, 1)
		c.Assert(err, check.IsNil)
	}

	mockMounterInput := make(chan *model.PolymorphicEvent, 10240)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < basicsTestMessageCount; i++ {
			var msg pipeline.Message
			if i%100 == 0 {
				// generates a control message
				msg = pipeline.TickMessage()
			} else {
				msg = pipeline.PolymorphicEventMessage(model.NewPolymorphicEvent(generateMockRawKV(uint64(i << 5))))
				msg.PolymorphicEvent.SetUpFinishedChan()
				select {
				case <-ctx.Done():
					return
				case mockMounterInput <- msg.PolymorphicEvent:
				}
			}
			sendMsg(p, msg)
		}
		msg := pipeline.PolymorphicEventMessage(model.NewResolvedPolymorphicEvent(0, (basicsTestMessageCount<<5)+1))
		sendMsg(p, msg)
		c.Assert(atomic.LoadInt64(&sentCount), check.Equals, int64(basicsTestMessageCount+1))
		log.Info("finished sending")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case event := <-mockMounterInput:
				event.Row = &model.RowChangedEvent{} // mocked row
				event.PrepareFinished()
			}
		}
	}()

	p.Wait()
	cancel()
	wg.Wait()
}
