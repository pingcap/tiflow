package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type fileWriter struct {
	filePath string
	fd       *os.File
	tid      int32
}

func (f *fileWriter) prepare() error {
	dir := filepath.Dir(f.filePath)
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(f.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o777)
	f.fd = file
	return err
}

func (f *fileWriter) write(_ *taskContext, r *Record) error {
	r.end = time.Now()
	str := []byte(r.toString())
	// ctx.stats[f.tid].recordCnt ++
	// ctx.stats[f.tid].totalLag += r.end.Sub(r.start)
	_, err := f.fd.Write(str)
	return err
}

type operator interface {
	next(ctx *taskContext, r *Record, idx int) ([]Chunk, bool, error)
	nextWantedInputIdx() int
	prepare() error
	close() error
}

type closeable interface {
	Close() error
}

type opReceive struct {
	flowID string
	addr   string
	data   chan *Record
	cache  Chunk
	conn   closeable
	errCh  chan error

	running      bool
	binlogClient pb.TestService_FeedBinlogClient
}

func (o *opReceive) nextWantedInputIdx() int { return -1 }

func (o *opReceive) close() error {
	return o.conn.Close()
}

func (o *opReceive) dial() (client pb.TestServiceClient, err error) {
	// get connection
	log.L().Info("dial to", zap.String("addr", o.addr))
	if test.GlobalTestFlag {
		conn, err := mock.Dial(o.addr)
		o.conn = conn
		if err != nil {
			return nil, errors.New("conn failed")
		}
		client = mock.NewTestClient(conn)
	} else {
		conn, err := grpc.Dial(o.addr, grpc.WithInsecure(), grpc.WithBlock())
		o.conn = conn
		if err != nil {
			return nil, errors.New("conn failed")
		}
		client = pb.NewTestServiceClient(conn)
	}
	return
}

func (o *opReceive) prepare() error {
	client, err := o.dial()
	if err != nil {
		return errors.New("conn failed")
	}
	// start receiving data
	// TODO: implement recover from a gtid point during failover.
	o.binlogClient, err = client.FeedBinlog(context.Background(), &pb.TestBinlogRequest{Gtid: 0})
	if err != nil {
		return errors.New("conn failed")
	}

	return nil
}

func (o *opReceive) next(ctx *taskContext, _ *Record, _ int) ([]Chunk, bool, error) {
	if !o.running {
		o.running = true
		go func() {
			for {
				record, err := o.binlogClient.Recv()
				if err != nil {
					o.errCh <- err
					log.L().Error("opReceive meet error", zap.Error(err))
					return
				}
				r := &Record{
					Tid:     record.Tid,
					Payload: record,
				}
				o.data <- r
				ctx.wake()
			}
		}()
	}
	o.cache = o.cache[:0]
	i := 0
	for ; i < 1024; i++ {
		select {
		case r := <-o.data:
			o.cache = append(o.cache, r)
		case err := <-o.errCh:
			return nil, true, err
		default:
			break
		}
	}
	if i == 0 {
		return nil, true, nil
	}
	return []Chunk{o.cache}, false, nil
}

type opSyncer struct{}

func (o *opSyncer) close() error { return nil }

// TODO communicate with master.
func (o *opSyncer) prepare() error { return nil }

func (o *opSyncer) syncDDL(ctx *taskContext) {
	time.Sleep(1 * time.Second)
	ctx.wake()
}

func (o *opSyncer) next(ctx *taskContext, r *Record, _ int) ([]Chunk, bool, error) {
	record := r.Payload.(*pb.Record)
	if record.Tp == pb.Record_DDL {
		go o.syncDDL(ctx)
		return nil, true, nil
	}
	return []Chunk{{r}}, false, nil
}

func (o *opSyncer) nextWantedInputIdx() int { return 0 }

type opSink struct {
	writer fileWriter
}

func (o *opSink) close() error { return nil }

func (o *opSink) prepare() error {
	return o.writer.prepare()
}

func (o *opSink) next(ctx *taskContext, r *Record, _ int) ([]Chunk, bool, error) {
	if test.GlobalTestFlag {
		//	log.L().Info("send record", zap.Int32("table", r.Tid), zap.Int32("pk", r.payload.(*pb.Record).Pk))
		ctx.testCtx.SendRecord(r)
		return nil, false, nil
	}
	return nil, false, o.writer.write(ctx, r)
}

func (o *opSink) nextWantedInputIdx() int { return 0 }
