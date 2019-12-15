// Copyright 2019 PingCAP, Inc.
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

package sink

import (
	"fmt"
	"github.com/pingcap/check"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
	"os"
	"sync"
)

type ConsumerSuite struct{}

var _ = check.Suite(&ConsumerSuite{})

type dummyCommit struct {
}

func (c *dummyCommit) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	fmt.Printf("topic=%s, partition=%d, offset=%d, meatadata=%s\n", topic, partition, offset, metadata)
}

var commiter = &dummyCommit{}

func (s ConsumerSuite) TestProcessMsg(c *check.C) {
	consumer := messageConsumer{
		sink:       &writerSink{os.Stdout},
		cdcCount:   2,
		persistSig: make(chan uint64, 13),
		session:    commiter,
	}
	go consumer.tick()
	//two partition and two cdc
	//cdc1
	consumer.processMsg(1, 1, newTestTxnMessage("cdc1", 1), commiter)
	consumer.processMsg(1, 2, newTestTxnMessage("cdc1", 4), commiter)
	consumer.processMsg(1, 3, newTestTxnMessage("cdc1", 2), commiter)
	consumer.processMsg(1, 4, newTestResolveRsMessage("cdc1", 3), commiter)
	consumer.processMsg(1, 5, newTestResolveRsMessage("cdc1", 8), commiter)
	consumer.processMsg(1, 6, newTestTxnMessage("cdc1", 9), commiter)
	consumer.processMsg(1, 6, newTestTxnMessage("cdc1", 11), commiter)
	//cdc2
	consumer.processMsg(2, 1, newTestTxnMessage("cdc2", 1), commiter)
	consumer.processMsg(2, 2, newTestTxnMessage("cdc2", 4), commiter)
	consumer.processMsg(2, 3, newTestTxnMessage("cdc2", 2), commiter)
	consumer.processMsg(2, 4, newTestResolveRsMessage("cdc2", 5), commiter)
	consumer.processMsg(2, 5, newTestResolveRsMessage("cdc2", 7), commiter)
	consumer.processMsg(2, 6, newTestTxnMessage("cdc2", 8), commiter)

	c.Check(consumer.metaGroup, check.IsNil)
	go consumer.processMsg(1, 7, newTestMataMsg([]string{"cdc1", "cdc2"}), commiter)
	consumer.processMsg(2, 7, newTestMataMsg([]string{"cdc1", "cdc2"}), commiter)
	c.Check(consumer.metaGroup, check.IsNil)
	for i := 0; i < 14; i++ {
		consumer.persistSig <- uint64(i)
	}
	consumer.tryPersistent(commiter)
	c.Check(getMapValueLength(&consumer.partitionMessageMap, int32(1)), check.Equals, 3)
	c.Check(getMapValueLength(&consumer.partitionMessageMap, int32(2)), check.Equals, 1)

	c.Check(1, check.Equals, getMapValueLength(&consumer.cdcResolveTsMap, "cdc1"))
	c.Check(0, check.Equals, getMapValueLength(&consumer.cdcResolveTsMap, "cdc2"))

	//meta msg again
	go consumer.processMsg(1, 8, newTestMataMsg([]string{"cdc1", "cdc2"}), commiter)
	consumer.processMsg(2, 8, newTestMataMsg([]string{"cdc1", "cdc2"}), commiter)
}

func newTestTxnMessage(cdcName string, ts uint64) *Message {
	return &Message{
		MsgType: TxnType,
		CdcID:   cdcName,
		Txn: &model.Txn{
			DMLs: []*model.DML{},
			DDL:  &model.DDL{},
			Ts:   ts,
		},
		TableInfos: map[string]*timodel.TableInfo{
			"test.user": {ID: 1},
		},
	}
}

func newTestResolveRsMessage(cdcName string, ts uint64) *Message {
	return &Message{
		MsgType:   ResolveTsType,
		CdcID:     cdcName,
		ResloveTs: ts,
	}
}

func newTestMataMsg(cdcList []string) *Message {
	return &Message{
		MsgType: MetaType,
		CdcList: cdcList,
	}
}

func getMapValueLength(m *sync.Map, key interface{}) int {
	v, _ := m.Load(key)
	return v.(*messageList).length()
}
