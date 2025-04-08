package main

import (
	"flag"
	"fmt"
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
)

func main() {
	// declare variables
	var (
		totalRows int
		txnNumber int
		duration  int
	)

	flag.IntVar(&totalRows, "rows", 200000000, "Total number of rows to write (default: 200M)")
	flag.IntVar(&txnNumber, "txn-number", 200000, "Number of rows per transaction (default: 200K)")
	flag.IntVar(&duration, "duration", 30, "Duration of the test in minutes (default: 30)")
	flag.Parse()

	var actualWriteRows atomic.Int64

	rowsPerTrans := totalRows / txnNumber
	numTransactions := txnNumber

	log.Printf("totalRows: %d, rowsPerTrans: %d, txnNumber: %d", totalRows, rowsPerTrans, numTransactions)

	// create temporary directory
	dbPath := filepath.Join(os.TempDir(), "pebble-test")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dbPath)

	// initialize database
	db, err := cdcpebble.OpenPebble(1, dbPath, &config.DBConfig{Count: 1}, nil)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	// create event sorter
	cf := model.ChangeFeedID{Namespace: "default", ID: "test"}
	eventSorter := cdcpebble.New(cf, []*pebble.DB{db})
	defer eventSorter.Close()

	if !eventSorter.IsTableBased() {
		fmt.Println("Sorter is not table based")
		os.Exit(1)
	}

	eventSorter.AddTable(1)
	resolvedTs := make(chan model.Ts, numTransactions*2)
	eventSorter.OnResolve(func(_ model.TableID, ts model.Ts) { resolvedTs <- ts })

	var (
		writeDuration atomic.Int64
		readDuration  atomic.Int64
	)

	// generate and write events
	fmt.Printf("start generating %d transactions, each with %d rows\n", numTransactions, rowsPerTrans)
	startTime := time.Now()
	value := []byte{1}
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

			// write events
			eventSorter.Add(1, events...)
			actualWriteRows.Add(int64(len(events)))
			// write resolved event
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

	var (
		readCount atomic.Int64
	)

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
				for {
					event, _, err := iter.Next()
					if err != nil {
						log.Printf("error when reading data: %v\n", err)
						os.Exit(1)
					}
					if event == nil {
						break
					}
					readCount.Add(1)

					if readCount.Load()%100000 == 0 {
						log.Printf("read %d rows\n", readCount.Load())
					}
				}
			case <-timer.C:
				log.Println("reading data timeout")
				os.Exit(1)
			}
		}
	}()

	wg.Wait()

	// compare the number of written and read rows
	if readCount.Load() != actualWriteRows.Load() {
		log.Printf("ERROR: data number mismatch: expected %d rows, actual read %d rows\n", actualWriteRows.Load(), readCount.Load())
	}

	log.Println("\nTest finished:")
	log.Printf("- write data: %d rows\n", actualWriteRows.Load())
	log.Printf("- read data: %d rows\n", readCount.Load())
	log.Printf("- write duration: %v\n", writeDuration.Load())
	log.Printf("- read duration: %v\n", readDuration.Load())
	log.Printf("- write speed: %.2f rows/second\n", float64(actualWriteRows.Load())/float64(writeDuration.Load()/1000))
	log.Printf("- read speed: %.2f rows/second\n", float64(readCount.Load())/float64(readDuration.Load()/1000))
}
