// Copyright 2022 PingCAP, Inc.
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

package eventsink

import (
	"github.com/pingcap/tiflow/cdc/sinkv2/event"
)

// EventSink is the interface for event sink.
type EventSink[E event.TableEvent] interface {
	// WriteEvents writes events to the sink.
	WriteEvents(rows ...*event.CallbackableEvent[E])
	// Close closes the sink.
	Close() error
}
