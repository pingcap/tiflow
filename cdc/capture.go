package cdc

import (
	"context"
	"sort"

	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/util"
)

type OpType int

const (
	OpTypeUnknow OpType = 0
	OpTypePut    OpType = 1
	OpTypeDelete OpType = 2
)

// Capture watch some span of KV and emit the entries to sink according to the ChangeFeedDetail
type Capture struct {
	pdCli        pd.Client
	watchs       []util.Span
	checkpointTS uint64
	encoder      Encoder
	detail       ChangeFeedDetail

	// errCh contains the return values of the puller
	errCh  chan error
	cancel context.CancelFunc

	// sink is the Sink to write rows to.
	// Resolved timestamps are never written by Capture
	sink Sink
}

type KVEntry struct {
	OpType OpType
	Key    []byte
	Value  []byte
	TS     uint64
}

type ResolvedSpan struct {
	Span      util.Span
	Timestamp uint64
}

func NewCapture(
	pdCli pd.Client,
	watchs []util.Span,
	checkpointTS uint64,
	detail ChangeFeedDetail,
) (c *Capture, err error) {
	encoder, err := getEncoder(detail.Opts)
	if err != nil {
		return nil, err
	}

	sink, err := getSink(detail.SinkURI, detail.Opts)
	if err != nil {
		return nil, err
	}

	c = &Capture{
		pdCli:        pdCli,
		watchs:       watchs,
		checkpointTS: checkpointTS,
		encoder:      encoder,
		sink:         sink,
		detail:       detail,
	}

	return
}

func (c *Capture) Start(ctx context.Context) (err error) {
	ctx, c.cancel = context.WithCancel(ctx)
	defer c.cancel()

	buf := MakeBuffer()

	puller := NewPuller(c.pdCli, c.checkpointTS, c.watchs, c.detail, buf)
	c.errCh = make(chan error, 2)
	go func() {
		err := puller.Run(ctx)
		c.errCh <- err
	}()

	// txns := collectRawTxns(ctx, buf.Get)

	rowsFn := kvsToRows(c.detail, buf.Get)
	emitFn := emitEntries(c.detail, c.watchs, c.encoder, c.sink, rowsFn)

	for {
		resolved, err := emitFn(ctx)
		if err != nil {
			select {
			case err = <-c.errCh:
			default:
			}
			return err
		}

		// TODO: forward resolved span to Frontier
		_ = resolved
	}
}

// Frontier handle all ResolvedSpan and emit resolved timestamp
type Frontier struct {
	// once all the span receive a resolved ts, it's safe to emit a changefeed level resolved ts
	spans   []util.Span
	detail  ChangeFeedDetail
	encoder Encoder
	sink    Sink
}

func NewFrontier(spans []util.Span, detail ChangeFeedDetail) (f *Frontier, err error) {
	encoder, err := getEncoder(detail.Opts)
	if err != nil {
		return nil, err
	}

	sink, err := getSink(detail.SinkURI, detail.Opts)
	if err != nil {
		return nil, err
	}

	f = &Frontier{
		spans:   spans,
		detail:  detail,
		encoder: encoder,
		sink:    sink,
	}

	return
}

func (f *Frontier) NotifyResolvedSpan(resolve ResolvedSpan) error {
	// TODO emit resolved timestamp once it's safe

	return nil
}

// RawTxn represents a complete collection of entries that belong to the same transaction
type RawTxn struct {
	ts      uint64
	entries []*KVEntry
}

// TODO: Add unit tests
func collectRawTxns(ctx context.Context, inputFn func(context.Context) (BufferEntry, error)) <-chan RawTxn {
	rawTxns := make(chan RawTxn)
	go func() {
		defer close(rawTxns)
		entryGroups := make(map[uint64][]*KVEntry)
		for {
			be, err := inputFn(ctx)
			if err != nil {
				return
			}
			if be.KV != nil {
				entryGroups[be.KV.TS] = append(entryGroups[be.KV.TS], be.KV)
			} else if be.Resolved != nil {
				resolvedTs := be.Resolved.Timestamp
				var readyTsList []uint64
				for ts := range entryGroups {
					if ts <= resolvedTs {
						readyTsList = append(readyTsList, ts)
					}
				}
				// TODO: Handle the case when readyTsList is empty
				sort.Slice(readyTsList, func(i, j int) bool {
					return readyTsList[i] < readyTsList[j]
				})
				for _, ts := range readyTsList {
					entries := entryGroups[ts]
					delete(entryGroups, ts)
					rawTxns <- RawTxn{ts, entries}
				}
			}
		}
	}()
	return rawTxns
}
