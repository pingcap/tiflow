package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine"
	cdcpebble "github.com/pingcap/tiflow/cdc/processor/sourcemanager/engine/pebble"
	"github.com/pingcap/tiflow/pkg/config"
)

func main() {
	// 定义命令行参数
	totalRows := flag.Int("rows", 200000000, "Total number of rows to write (default: 200M)")
	txnNumber := flag.Int("txn-number", 200000, "Number of rows per transaction (default: 200K)")
	duration := flag.Int("duration", 30, "Duration of the test in minutes (default: 30)")
	flag.Parse()

	var actualWriteRows atomic.Int64
	// 计算事务数量
	rowsPerTrans := *totalRows / *txnNumber

	log.Printf("totalRows: %d, rowsPerTrans: %d, txnNumber: %d", *totalRows, rowsPerTrans, *txnNumber)

	// 创建临时目录
	dbPath := filepath.Join(os.TempDir(), "pebble-test")
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		fmt.Printf("Failed to create temp directory: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(dbPath)

	// 初始化数据库
	db, err := cdcpebble.OpenPebble(1, dbPath, &config.DBConfig{Count: 1}, nil)
	if err != nil {
		fmt.Printf("Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	// 初始化 sorter
	cf := model.ChangeFeedID{Namespace: "default", ID: "test"}
	s := cdcpebble.New(cf, []*pebble.DB{db})
	defer s.Close()

	if !s.IsTableBased() {
		fmt.Println("Sorter is not table based")
		os.Exit(1)
	}

	tableID := model.TableID(1)
	bTableID := []byte{byte(tableID)}
	s.AddTable(tableID)
	resolvedTs := make(chan model.Ts)
	s.OnResolve(func(_ model.TableID, ts model.Ts) { resolvedTs <- ts })

	// 生成并写入事件
	fmt.Printf("开始生成 %d 个事务，每个事务 %d 行数据\n", *txnNumber, rowsPerTrans)
	startTime := time.Now()

	for i := 0; i < *txnNumber; i++ {
		events := make([]*model.PolymorphicEvent, 0, rowsPerTrans)
		commitTs := model.Ts(i + 1)

		// 计算这个事务实际需要写入的行数
		rowsToWrite := rowsPerTrans
		if i == *txnNumber-1 && *totalRows%rowsPerTrans != 0 {
			rowsToWrite = *totalRows % rowsPerTrans
		}
		actualWriteRows.Add(int64(rowsToWrite))

		for j := 0; j < rowsToWrite; j++ {
			event := model.NewPolymorphicEvent(&model.RawKVEntry{
				OpType:  model.OpTypePut,
				Key:     bTableID,
				StartTs: commitTs - 1,
				CRTs:    commitTs,
			})
			events = append(events, event)
		}

		s.Add(tableID, events...)
		s.Add(tableID, model.NewResolvedPolymorphicEvent(0, commitTs))

		if (i+1)%10 == 0 {
			fmt.Printf("已完成 %d 个事务的写入\n", i+1)
		}
	}

	writeDuration := time.Since(startTime)
	fmt.Printf("数据写入完成，耗时: %v\n", writeDuration)

	// 读取并验证数据
	fmt.Println("开始读取数据...")
	readStartTime := time.Now()

	var (
		readCount int64
	)

	timer := time.NewTimer(time.Duration(*duration) * time.Minute)
	select {
	case ts := <-resolvedTs:
		iter := s.FetchByTable(1, engine.Position{}, engine.Position{CommitTs: ts, StartTs: ts - 1})
		for {
			event, _, err := iter.Next()
			if err != nil {
				fmt.Printf("读取数据时发生错误: %v\n", err)
				os.Exit(1)
			}
			if event == nil {
				break
			}
			atomic.AddInt64(&readCount, 1)

			if readCount%100000 == 0 {
				fmt.Printf("已读取 %d 条数据\n", readCount)
			}
		}
	case <-timer.C:
		fmt.Println("读取数据超时")
		os.Exit(1)
	}

	readDuration := time.Since(readStartTime)
	fmt.Printf("数据读取完成，耗时: %v\n", readDuration)

	// 验证数据完整性
	if readCount != actualWriteRows.Load() {
		fmt.Printf("数据不完整: 期望 %d 条，实际读取 %d 条\n", actualWriteRows.Load(), readCount)
		os.Exit(1)
	}

	fmt.Println("\n测试完成:")
	fmt.Printf("- 写入数据: %d 条\n", actualWriteRows.Load())
	fmt.Printf("- 读取数据: %d 条\n", readCount)
	fmt.Printf("- 写入耗时: %v\n", writeDuration)
	fmt.Printf("- 读取耗时: %v\n", readDuration)
	fmt.Printf("- 写入速度: %.2f 行/秒\n", float64(actualWriteRows.Load())/writeDuration.Seconds())
	fmt.Printf("- 读取速度: %.2f 行/秒\n", float64(readCount)/readDuration.Seconds())
}
