// Copyright 2024 PingCAP, Inc.
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

package simple

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
)

var event *model.RowChangedEvent

func eventGenerator() *model.RowChangedEvent {
	if event == nil {
		t := &testing.T{}
		_, insertEvent, _, _ := utils.NewLargeEvent4Test(t, config.GetDefaultReplicaConfig())
		event = insertEvent
	}
	return event
}

// Note(dongmen): Below is the result of running the benchmark at 2024-4-22.
// goos: linux
// goarch: amd64
// pkg: github.com/pingcap/tiflow/pkg/sink/codec/simple
// cpu: Intel(R) Xeon(R) Gold 6240 CPU @ 2.60GHz
// BenchmarkMarshalRowChangedEvent-16    	   47527	     29011 ns/op	    8161 B/op	     130 allocs/op
func BenchmarkMarshalRowChangedEvent(b *testing.B) {
	codecConfig := common.NewConfig(config.ProtocolSimple)
	avroMarshaller, err := newAvroMarshaller(codecConfig, string(avroSchemaBytes))
	if err != nil {
		panic(err)
	}
	rowChangeEvent := eventGenerator()
	if rowChangeEvent == nil {
		panic(errors.New("event is nil"))
	}
	handleKeyOnly := false
	claimCheckFileName := ""

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := avroMarshaller.MarshalRowChangedEvent(
			rowChangeEvent,
			handleKeyOnly,
			claimCheckFileName)
		if err != nil {
			panic(errors.Trace(err))
		}
	}
}
