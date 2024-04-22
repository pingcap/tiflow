package simple

import (
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
)

var event *model.RowChangedEvent
var lock = sync.Mutex{}

func eventGenerator() *model.RowChangedEvent {
	lock.Lock()
	defer lock.Unlock()
	if event == nil {
		t := &testing.T{}
		_, insertEvent, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
		event = insertEvent
	}
	return event
}

func BenchmarkMarshalRowChangedEvent(b *testing.B) {
	codecConfig := common.NewConfig(config.ProtocolSimple)
	avroMarshaller, err := newAvroMarshaller(codecConfig, string(avroSchemaBytes))
	if err != nil {
		panic(err)
	}
	e := eventGenerator()
	if e == nil {
		panic(errors.New("event is nil"))
	}
	// initialize your event here
	handleKeyOnly := false   // set your handleKeyOnly value
	claimCheckFileName := "" // set your claimCheckFileName

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		//log.Info("marshal row changed event", zap.Any("event", e))
		_, err := avroMarshaller.MarshalRowChangedEvent(e, handleKeyOnly, claimCheckFileName)
		if err != nil {
			panic(errors.Trace(err))
		}
	}
}
