package partition

import (
	"github.com/pingcap/tiflow/cdc/model"
)

// PulsarTransportDispatcher is a partition dispatcher which dispatches events based transport
type PulsarTransportDispatcher struct {
	partitionKey string
}

// NewPulsarTransportDispatcher creates a TableDispatcher.
func NewPulsarTransportDispatcher(partitionKey string) *PulsarTransportDispatcher {
	return &PulsarTransportDispatcher{
		partitionKey: partitionKey,
	}
}

// DispatchRowChangedEvent returns the target partition to which
// a row changed event should be dispatched.
func (t *PulsarTransportDispatcher) DispatchRowChangedEvent(*model.RowChangedEvent, int32) (int32, *string) {
	return 0, &t.partitionKey
}
