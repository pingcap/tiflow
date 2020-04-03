package model

type PolymorphicEvent struct {
	Ts       uint64
	RawKV    *RawKVEntry
	Row      *RowChangedEvent
	finished chan struct{}
}

func NewPolymorphicEvent(rawKV *RawKVEntry) *PolymorphicEvent {
	return &PolymorphicEvent{
		Ts:       rawKV.Ts,
		RawKV:    rawKV,
		finished: make(chan struct{}),
	}
}

func NewResolvedPolymorphicEvent(ts uint64) *PolymorphicEvent {
	return &PolymorphicEvent{
		Ts:       ts,
		RawKV:    &RawKVEntry{Ts: ts, OpType: OpTypeResolved},
		Row:      &RowChangedEvent{Ts: ts, Resolved: true},
		finished: nil,
	}
}

func (e *PolymorphicEvent) PrepareFinished() {
	if e.finished != nil {
		close(e.finished)
	}
}

func (e *PolymorphicEvent) WaitPrepare() {
	if e.finished == nil {
		<-e.finished
	}
}
