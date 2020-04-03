package model

// PolymorphicEvent describes a event can be in multiple states
type PolymorphicEvent struct {
	Ts       uint64
	RawKV    *RawKVEntry
	Row      *RowChangedEvent
	finished chan struct{}
}

// NewPolymorphicEvent creates a new PolymorphicEvent with a raw KV
func NewPolymorphicEvent(rawKV *RawKVEntry) *PolymorphicEvent {
	if rawKV.OpType == OpTypeResolved {
		return NewResolvedPolymorphicEvent(rawKV.Ts)
	}
	return &PolymorphicEvent{
		Ts:       rawKV.Ts,
		RawKV:    rawKV,
		finished: make(chan struct{}),
	}
}

// NewResolvedPolymorphicEvent creates a new PolymorphicEvent with the resolved ts
func NewResolvedPolymorphicEvent(ts uint64) *PolymorphicEvent {
	return &PolymorphicEvent{
		Ts:       ts,
		RawKV:    &RawKVEntry{Ts: ts, OpType: OpTypeResolved},
		Row:      &RowChangedEvent{Ts: ts, Resolved: true},
		finished: nil,
	}
}

// PrepareFinished marks the prepare process is finished
func (e *PolymorphicEvent) PrepareFinished() {
	if e.finished != nil {
		close(e.finished)
	}
}

// WaitPrepare waits for prepare process finished
func (e *PolymorphicEvent) WaitPrepare() {
	if e.finished != nil {
		<-e.finished
	}
}
