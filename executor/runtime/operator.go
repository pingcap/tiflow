package runtime

import (
	"context"
	"errors"
	"hash/crc32"
	"os"

	//"io/fs"
	//"io/ioutil"
	"github.com/hanfei1991/microcosom/pkg/workerpool"
	"sync"
	"time"

	"github.com/hanfei1991/microcosom/pb"

	"google.golang.org/grpc"
)

type fileWriter struct {
	mu       sync.Mutex
	filePath string
	fd       *os.File
	tid      int
}

func (f *fileWriter) prepare() error {
	file, err := os.OpenFile(f.filePath, os.O_APPEND| os.O_CREATE| os.O_WRONLY, 0777)
	f.fd = file
	return err
}

func (f *fileWriter) write(ctx *taskContext, r *Record) {
	r.end = time.Now()
	str := []byte(r.toString())
//	f.mu.Lock()
//	defer f.mu.Unlock()
	ctx.stats[f.tid].recordCnt ++
	ctx.stats[f.tid].totalLag += r.end.Sub(r.start)
	_, err := f.fd.Write(str)
	if err != nil {
		panic(err)
	}
}

type tableStats struct {
	totalLag  time.Duration
	recordCnt int
}

type taskContext struct {
	ioPool workerpool.AsyncPool
	tableCnt int32
	stats []tableStats
}

type operator interface {
	next(ctx *taskContext, r []*Record) ([][]*Record, bool)
	prepare(ctx *taskContext) error
}

type opReceive struct {
	addr  string
	data  chan *Record
	cache [][]*Record
}

func (o *opReceive) prepare(ctx *taskContext) error {
	// get connection
	conn, err := grpc.Dial(o.addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return errors.New("conn failed")
	}
	client := pb.NewTmpServiceClient(conn)
	// start receiving data
	stream, err := client.EventFeed(context.Background(), &pb.Request{MaxTid: ctx.tableCnt})
	if err != nil {
		return errors.New("conn failed")
	}
	go func() {
		for {
			record, err := stream.Recv()
			if err != nil {
				panic(err)
			}
			ts, err := time.Parse(time.RFC3339Nano, string(record.StartTs))
			if err != nil {
				panic(err)
			}
			r := &Record{
				start:   ts,
				payload: record.Payload,
				tid:     record.Tid,
			}
			o.data <- r
		}
	}()
	return nil
}

func (o *opReceive) next(ctx *taskContext, _ []*Record) ([][]*Record, bool) {
	for i := range o.cache {
		o.cache[i] = o.cache[i][:0]
	}
	i := 0
	for ; i < 1024; i++ {
		select {
		case r := <-o.data:
			o.cache[r.tid] = append(o.cache[r.tid], r)
		default:
			break
		}
	}
	if i == 0 {
		return nil, false 
	} else {
		return o.cache, false
	}
}

type opHash struct {
}

func (o *opHash) prepare(ctx *taskContext) error { return nil }

func (o *opHash) next(ctx *taskContext, records []*Record) ([][]*Record, bool) {
	for _, record := range records {
		record.hashVal = crc32.ChecksumIEEE(record.payload)
	}
	return [][]*Record{records}, false
}

type opSink struct {
	writer fileWriter
}

func (o *opSink) prepare(_ *taskContext) error {
	return o.writer.prepare()
}

func (o *opSink) next(ctx *taskContext, records []*Record) ([][]*Record, bool) {
	if len(records) == 0 {
		return nil, true
	}
	//ctx.ioPool.Go(context.Background(), func() {
		for _, r := range records {
			o.writer.write(ctx, r)
		}
	//})
	return nil, false
}
