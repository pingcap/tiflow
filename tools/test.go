package main

import (
	"context"
	"encoding/base64"
	"flag"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	cdcpebble "github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/pebble"
	"github.com/pingcap/tiflow/pkg/config"
	"golang.org/x/time/rate"
)

// How to run this program:
// go run test.go -rows=1000000000 -txn-number=100000 -min-ts-interval 200 -row-per-second 20000

func main() {
	// declare variables
	var (
		totalRows     int
		txnNumber     int
		duration      int
		minTsInterval int // min-ts-interval in milliseconds
		rowPerSecond  int
	)

	flag.IntVar(&totalRows, "rows", 200000000, "Total number of rows to write (default: 200M)")
	flag.IntVar(&txnNumber, "txn-number", 200000, "Number of rows per transaction (default: 200K)")
	flag.IntVar(&duration, "duration", 30, "Duration of the test in minutes (default: 30)")
	flag.IntVar(&minTsInterval, "min-ts-interval", 1000, "Min ts interval in milliseconds (default: 1000, that is 1 second)")
	flag.IntVar(&rowPerSecond, "row-per-second", 20000, "Number of rows to write per second (default: 20K)")
	flag.Parse()

	rowsPerTrans := totalRows / txnNumber
	numTransactions := txnNumber

	if rowPerSecond < rowsPerTrans {
		log.Printf("rowPerSecond %d is less than rowsPerTrans %d, set rowPerSecond to rowsPerTrans", rowPerSecond, rowsPerTrans)
		rowPerSecond = rowsPerTrans
	}

	log.Printf("totalRows: %d, rowsPerTrans: %d, txnNumber: %d, minTsInterval: %d, rowPerSecond: %d", totalRows, rowsPerTrans, numTransactions, minTsInterval, rowPerSecond)

	resolvedTsRate := 1000 / minTsInterval
	resolvedTsRateLimit := rate.NewLimiter(rate.Limit(resolvedTsRate), 1)
	writeRowPerSecondRateLimit := rate.NewLimiter(rate.Limit(rowPerSecond), rowPerSecond)
	readRowPerSecondRateLimit := rate.NewLimiter(rate.Limit(rowPerSecond), rowPerSecond)

	// create temporary directory
	dbPath := filepath.Join(os.TempDir(), "pebble-test")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		log.Printf("Failed to create temp directory: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dbPath)

	// initialize database
	db, err := cdcpebble.OpenPebble(1, dbPath, &config.DBConfig{Count: 1}, nil)
	if err != nil {
		log.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	// create event sorter
	cf := model.ChangeFeedID{Namespace: "default", ID: "test"}
	eventSorter := cdcpebble.New(cf, []*pebble.DB{db})
	defer eventSorter.Close()

	if !eventSorter.IsTableBased() {
		log.Println("Sorter is not table based")
		os.Exit(1)
	}

	eventSorter.AddTable(1)
	resolvedTs := make(chan model.Ts, numTransactions*2)
	eventSorter.OnResolve(func(_ model.TableID, ts model.Ts) { resolvedTs <- ts })

	var (
		writeDuration   atomic.Int64
		readDuration    atomic.Int64
		writeTxnNumber  atomic.Int64
		readTxnNumber   atomic.Int64
		actualWriteRows atomic.Int64
		actualReadRows  atomic.Int64
	)

	// generate and write events
	log.Printf("start generating %d transactions, each with %d rows\n", numTransactions, rowsPerTrans)
	startTime := time.Now()
	decodedValue, err := base64.StdEncoding.DecodeString("h6dvcF90eXBlAaNrZXnERHSAAAAAAAAAUF9yATEzMjEzNTE5/zY4MDkyNzM5/zEzAAAAAAAA+QExAAAAAAAAAPgEGbYmAAAAAAABMTEwAAAAAAD6pXZhbHVlxD6AAAUAAAABAgMEBhIAFQAWACEAKQAxMzIxMzUxOTY4MDkyNzM5MTMxMTAxEgKAAAAAAAAAAQMAAABEYie2GalvbGRfdmFsdWXEAKhzdGFydF90c88GWF9RNJgABqRjcnRzzwZYX1E0mAAHqXJlZ2lvbl9pZAo=")
	if err != nil {
		log.Fatalf("Failed to decode base64 string: %v", err)
	}
	value := decodedValue

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
			writeDuration.Store(time.Since(startTime).Milliseconds())
		}()

		for i := 0; i < numTransactions; i++ {
			events := make([]*model.PolymorphicEvent, 0, rowsPerTrans)
			commitTs := model.Ts(i + 1)
			// generate events
			ctx := context.Background()
			for j := 0; j < rowsPerTrans; j++ {
				event := model.NewPolymorphicEvent(&model.RawKVEntry{
					OpType:  model.OpTypePut,
					Key:     []byte{1},
					Value:   value,
					StartTs: commitTs - 1,
					CRTs:    commitTs,
				})
				events = append(events, event)
			}

			writeRowPerSecondRateLimit.WaitN(ctx, len(events))
			eventSorter.Add(1, events...)
			actualWriteRows.Add(int64(len(events)))
			writeTxnNumber.Add(1)

			// write resolved event
			resolvedTsRateLimit.WaitN(ctx, 1)
			eventSorter.Add(model.TableID(1), model.NewResolvedPolymorphicEvent(0, commitTs))

			if (i+1)%10000 == 0 {
				log.Printf("write %d transactions\n", i+1)
			}
		}
		log.Printf("Finish write %d transactions, total rows: %d \n", numTransactions, actualWriteRows.Load())
		resolvedTs <- math.MaxUint64
	}()

	// read and verify data
	log.Println("start reading data...")
	readStartTime := time.Now()

	timer := time.NewTimer(time.Duration(duration) * time.Minute)
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
			readDuration.Store(time.Since(readStartTime).Milliseconds())
		}()
		for {
			select {
			case ts := <-resolvedTs:
				if ts == math.MaxUint64 {
					log.Println("Received the end signal from writer, exit reading")
					return
				}

				iter := eventSorter.FetchByTable(1, engine.Position{}, engine.Position{CommitTs: ts, StartTs: ts - 1})

				readTxnNumber.Add(1)
				count := 0
				ctx := context.Background()
				for {
					event, _, err := iter.Next()
					if err != nil {
						// Only log the error, don't exit the program.
						log.Printf("error when reading data: %v\n", err)
					}
					if event == nil {
						break
					}
					count++
					actualReadRows.Add(1)

					if actualReadRows.Load()%100000 == 0 {
						log.Printf("read %d rows\n", actualReadRows.Load())
					}
				}

				readRowPerSecondRateLimit.WaitN(ctx, count)
				readTxnNumber.Add(1)
			case <-timer.C:
				log.Println("reading data timeout")
				os.Exit(1)
			}
		}
	}()

	wg.Wait()

	// compare the number of written and read rows
	if actualReadRows.Load() != actualWriteRows.Load() {
		log.Printf("ERROR: data number mismatch: write %d rows, read %d rows, write txn number: %d, read txn number: %d\n", actualWriteRows.Load(), actualReadRows.Load(), writeTxnNumber.Load(), readTxnNumber.Load())
	}

	log.Println("\nTest finished:")
	log.Printf("- write data: %d rows\n", actualWriteRows.Load())
	log.Printf("- read data: %d rows\n", actualReadRows.Load())
	log.Printf("- write duration: %v\n", writeDuration.Load())
	log.Printf("- read duration: %v\n", readDuration.Load())
	log.Printf("- write speed: %.2f rows/second, %.2f txn/second\n", float64(actualWriteRows.Load())/float64(writeDuration.Load()/1000), float64(writeTxnNumber.Load())/float64(writeDuration.Load()/1000))
	log.Printf("- read speed: %.2f rows/second, %.2f txn/second\n", float64(actualReadRows.Load())/float64(readDuration.Load()/1000), float64(readTxnNumber.Load())/float64(readDuration.Load()/1000))
}
