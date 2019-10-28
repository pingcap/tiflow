package roles

import (
	"math"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-cdc/cdc/schema"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"go.uber.org/zap"
)

type processorSuite struct{}

type mockTSRWriter struct {
	resolvedTS   uint64
	checkpointTS uint64
}

func (s *mockTSRWriter) WriteResolvedTS(resolvedTS uint64) error {
	log.Info("write", zap.Uint64("localResolvedTS", resolvedTS))
	s.resolvedTS = resolvedTS
	return nil
}

func (s *mockTSRWriter) WriteCheckpointTS(checkpointTS uint64) error {
	log.Info("write", zap.Uint64("checkpointTS", checkpointTS))
	s.checkpointTS = checkpointTS
	return nil
}

func (s *mockTSRWriter) ReadGlobalResolvedTS() (uint64, error) {
	return s.resolvedTS, nil
}

var _ = check.Suite(&processorSuite{})

type inputEntry struct {
	ts           uint64
	isResolvedTs bool
}

func createInputChan(table schema.TableName, cases []inputEntry) (txnChan chan txn.RawTxn, resolvedTSChan chan uint64) {
	var rawTxns []txn.RawTxn
	var resolvedTSs []uint64
	for _, c := range cases {
		if c.isResolvedTs {
			resolvedTSs = append(resolvedTSs, c.ts)
		} else {
			rawTxns = append(rawTxns, txn.RawTxn{
				TS: c.ts,
			})
		}
	}
	txnChan = make(chan txn.RawTxn)
	resolvedTSChan = make(chan uint64)
	go func() {
		resolvedTSIndex := 0
		if len(resolvedTSs) > resolvedTSIndex {
			log.Info("input", zap.Uint64("resolvedTS", resolvedTSs[resolvedTSIndex]), zap.Reflect("table", table))
			resolvedTSChan <- resolvedTSs[resolvedTSIndex]
		}
		for _, rawTxn := range rawTxns {
			if len(resolvedTSs) > resolvedTSIndex && resolvedTSs[resolvedTSIndex] < rawTxn.TS {
				resolvedTSIndex++
				if len(resolvedTSs) > resolvedTSIndex {
					log.Info("input", zap.Uint64("resolvedTS", resolvedTSs[resolvedTSIndex]), zap.Reflect("table", table))
					resolvedTSChan <- resolvedTSs[resolvedTSIndex]
				}
			}
			txnChan <- rawTxn
		}
	}()
	return
}

func (s *processorSuite) TestProcessor(c *check.C) {
	p := NewProcessor(&mockTSRWriter{})
	t1 := schema.TableName{Schema: "s", Table: "t1"}
	t2 := schema.TableName{Schema: "s", Table: "t2"}
	txnChan1, resolvedTSChan1 := createInputChan(t1, []inputEntry{
		{ts: 1},
		{ts: 2},
		{ts: 3},
		{ts: 4, isResolvedTs: true},
		{ts: 5},
		{ts: 6},
		{ts: 7, isResolvedTs: true},
		{ts: 8},
		{ts: 9, isResolvedTs: true},
		{ts: uint64(math.MaxUint64)}, // terminator
	})
	txnChan2, resolvedTSChan2 := createInputChan(t2, []inputEntry{
		{ts: 2},
		{ts: 4},
		{ts: 6, isResolvedTs: true},
		{ts: 7},
		{ts: 8},
		{ts: 9},
		{ts: 10, isResolvedTs: true},
		{ts: uint64(math.MaxUint64)}, // terminator
	})
	closed := make(chan struct{})
	go func() {
		maxAcceptedTS := uint64(0)
		lastResolvedTS := uint64(0)
		for {
			e, ok := <-p.ResolvedChan()
			if e.Typ == ProcessorEntryDMLS {
				c.Assert(e.TS, check.Greater, lastResolvedTS)
				if maxAcceptedTS < e.TS {
					maxAcceptedTS = e.TS
				}
			} else {
				c.Assert(e.TS, check.GreaterEqual, maxAcceptedTS)
				lastResolvedTS = e.TS
			}
			if !ok {
				log.Info("TestProcessor checker function is exited")
			}
			log.Info("resolved", zap.Reflect("entry", e))
			p.ExecutedChan() <- e
			if e.Typ == ProcessorEntryResolved && e.TS == 9 {
				p.Close()
				close(closed)
				return
			}
		}
	}()
	p.SetInputChan(t1, txnChan1, resolvedTSChan1)
	p.SetInputChan(t2, txnChan2, resolvedTSChan2)
	<-closed
}
