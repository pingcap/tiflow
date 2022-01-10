package benchmark

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hanfei1991/microcosm/executor/runtime"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/test"
	"github.com/hanfei1991/microcosm/test/mock"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

type baseOp struct{}

func (o *baseOp) Pause() error {
	return nil
}

type fileWriter struct {
	filePath string
	fd       *os.File
	tid      int32
}

func (f *fileWriter) Prepare() (runtime.TaskRescUnit, error) {
	dir := filepath.Dir(f.filePath)
	err := os.MkdirAll(dir, 0o755)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(f.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o777)
	if err != nil {
		return nil, err
	}
	f.fd = file
	return runtime.NewSimpleTRU(model.Benchmark), err
}

func sprintPayload(r *pb.Record) string {
	var buf bytes.Buffer
	buf.WriteString("tid ")
	buf.WriteString(strconv.FormatInt(int64(r.Tid), 10))
	buf.WriteString(", pk")
	buf.WriteString(strconv.FormatInt(int64(r.Pk), 10))
	buf.WriteString(", time tracer ")
	for _, ts := range r.TimeTracer {
		buf.WriteString(strconv.FormatInt(ts, 10) + " ")
	}
	return buf.String()
}

func sprintRecord(r *runtime.Record) string {
	start := time.Unix(0, r.Payload.(*pb.Record).TimeTracer[0])
	var buf bytes.Buffer
	buf.WriteString("flowID ")
	buf.WriteString(r.FlowID)
	buf.WriteString(" start ")
	buf.WriteString(start.String())
	buf.WriteString(" end ")
	buf.WriteString(r.End.String())
	buf.WriteString(" payload: ")
	buf.WriteString(sprintPayload(r.Payload.(*pb.Record)))
	buf.WriteString("\n")
	return buf.String()
}

func (f *fileWriter) writeStats(s *recordStats) error {
	str := []byte(s.String())
	_, err := f.fd.Write(str)
	return err
}

func (f *fileWriter) write(s *recordStats, r *runtime.Record) error {
	str := []byte(sprintRecord(r))
	start := time.Unix(0, r.Payload.(*pb.Record).TimeTracer[0])
	s.cnt++
	s.totalLag += r.End.Sub(start)
	_, err := f.fd.Write(str)
	return err
}

type Closeable interface {
	Close() error
}

type opReceive struct {
	baseOp
	flowID string
	addr   string
	data   chan *runtime.Record
	cache  runtime.Chunk
	conn   Closeable
	errCh  chan error

	running      bool
	binlogClient pb.TestService_FeedBinlogClient

	ctx    context.Context
	cancel context.CancelFunc
}

func (o *opReceive) NextWantedInputIdx() int { return runtime.DontNeedData }

func (o *opReceive) Close() error {
	o.cancel()
	return o.conn.Close()
}

func (o *opReceive) dial() (client pb.TestServiceClient, err error) {
	// get connection
	log.L().Info("dial to", zap.String("addr", o.addr))
	if test.GetGlobalTestFlag() {
		conn, err := mock.Dial(o.addr)
		o.conn = conn
		if err != nil {
			return nil, errors.New("conn failed")
		}
		client = mock.NewTestClient(conn)
	} else {
		conn, err := grpc.Dial(o.addr, grpc.WithInsecure(), grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}))
		o.conn = conn
		if err != nil {
			return nil, errors.New("conn failed")
		}
		client = pb.NewTestServiceClient(conn)
	}
	return
}

func (o *opReceive) Prepare(_ *runtime.TaskContext) (runtime.TaskRescUnit, error) {
	o.ctx, o.cancel = context.WithCancel(context.Background())
	return runtime.NewSimpleTRU(model.Benchmark), nil
}

func (o *opReceive) connect() error {
	client, err := o.dial()
	if err != nil {
		return errors.New("conn failed")
	}
	// start receiving data
	// TODO: implement recover from a gtid point during failover.
	o.binlogClient, err = client.FeedBinlog(o.ctx, &pb.TestBinlogRequest{Gtid: 0})
	if err != nil {
		return errors.New("conn failed")
	}

	return nil
}

func (o *opReceive) Next(ctx *runtime.TaskContext, _ *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	if !o.running {
		o.running = true
		go func() {
			err := o.connect()
			if err != nil {
				o.errCh <- err
				log.L().Error("opReceive meet error", zap.Error(err))
				return
			}
			for {
				record, err := o.binlogClient.Recv()
				if err != nil {
					o.errCh <- err
					log.L().Error("opReceive meet error", zap.Error(err))
					return
				}
				r := &runtime.Record{
					Tid:     record.Tid,
					Payload: record,
				}
				o.data <- r
				ctx.Wake()
			}
		}()
	}
	o.cache = o.cache[:0]
	i := 0
	for ; i < 1024; i++ {
		noMoreData := false
		select {
		case r := <-o.data:
			payload := r.Payload.(*pb.Record)
			payload.TimeTracer = append(payload.TimeTracer, time.Now().UnixNano())
			o.cache = append(o.cache, r)
		case err := <-o.errCh:
			return nil, true, err
		default:
			noMoreData = true
		}
		if noMoreData {
			break
		}
	}
	if i == 0 {
		return nil, true, nil
	}
	return []runtime.Chunk{o.cache}, false, nil
}

type opSyncer struct {
	baseOp
}

func (o *opSyncer) Close() error { return nil }

// TODO communicate with master.
func (o *opSyncer) Prepare(_ *runtime.TaskContext) (runtime.TaskRescUnit, error) {
	return runtime.NewSimpleTRU(model.Benchmark), nil
}

func (o *opSyncer) syncDDL(ctx *runtime.TaskContext) {
	time.Sleep(1 * time.Second)
	ctx.Wake()
}

func (o *opSyncer) Next(ctx *runtime.TaskContext, r *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	record := r.Payload.(*pb.Record)
	record.TimeTracer = append(record.TimeTracer, time.Now().UnixNano())
	if record.Tp == pb.Record_DDL {
		go o.syncDDL(ctx)
		return nil, true, nil
	}
	return []runtime.Chunk{{r}}, false, nil
}

func (o *opSyncer) NextWantedInputIdx() int { return 0 }

type recordStats struct {
	totalLag time.Duration
	cnt      int64
}

func (s *recordStats) String() string {
	return fmt.Sprintf("total record %d, average lantency %.3f ms", s.cnt, float64(s.totalLag.Milliseconds())/float64(s.cnt))
}

type opSink struct {
	baseOp
	writer fileWriter
	stats  *recordStats
}

func (o *opSink) Close() error {
	return o.writer.writeStats(o.stats)
}

func (o *opSink) Prepare(_ *runtime.TaskContext) (runtime.TaskRescUnit, error) {
	o.stats = new(recordStats)
	return o.writer.Prepare()
}

func (o *opSink) Next(ctx *runtime.TaskContext, r *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	r.End = time.Now()
	if test.GetGlobalTestFlag() {
		//	log.L().Info("send record", zap.Int32("table", r.Tid), zap.Int32("pk", r.payload.(*pb.Record).Pk))
		ctx.TestCtx.SendRecord(r)
		return nil, false, nil
	}
	return nil, false, o.writer.write(o.stats, r)
}

func (o *opSink) NextWantedInputIdx() int { return 0 }

type opProducer struct {
	baseOp
	tid       int32
	pk        int32
	schemaVer int32
	dataCnt   int32

	ddlFrequency int32
	outputCnt    int

	checkpoint time.Time
}

func (o *opProducer) Close() error { return nil }

func (o *opProducer) Prepare(_ *runtime.TaskContext) (runtime.TaskRescUnit, error) {
	return runtime.NewSimpleTRU(model.Benchmark), nil
}

func (o *opProducer) NextWantedInputIdx() int { return runtime.DontNeedData }

func (o *opProducer) Next(ctx *runtime.TaskContext, _ *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	outputData := make([]runtime.Chunk, o.outputCnt)
	binlogID := 0
	if o.checkpoint.Add(50 * time.Millisecond).After(time.Now()) {
		return nil, true, nil
	}
	for i := 0; i < 128; i++ {
		if o.pk >= o.dataCnt {
			return outputData, true, nil
		}
		o.pk++
		start := time.Now()
		if o.pk%o.ddlFrequency == 0 {
			o.schemaVer++
			for i := range outputData {
				payload := &pb.Record{
					Tp:         pb.Record_DDL,
					Tid:        o.tid,
					SchemaVer:  o.schemaVer,
					TimeTracer: []int64{start.UnixNano()},
				}
				r := runtime.Record{
					Tid:     o.tid,
					Payload: payload,
				}
				outputData[i] = append(outputData[i], &r)
			}
		}
		payload := &pb.Record{
			Tp:         pb.Record_Data,
			Tid:        o.tid,
			SchemaVer:  o.schemaVer,
			Pk:         o.pk,
			TimeTracer: []int64{start.UnixNano()},
		}
		r := runtime.Record{
			Tid:     o.tid,
			Payload: payload,
		}
		outputData[binlogID] = append(outputData[binlogID], &r)
		binlogID = (binlogID + 1) % o.outputCnt
	}
	if !test.GetGlobalTestFlag() {
		o.checkpoint = time.Now()
		go func() {
			time.Sleep(55 * time.Millisecond)
			ctx.Wake()
		}()
		return outputData, true, nil
	}
	return outputData, false, nil
}

type stoppable interface {
	Stop()
}

type opBinlog struct {
	baseOp
	binlogChan chan *runtime.Record
	wal        []*runtime.Record
	addr       string

	server      stoppable
	cacheRecord *runtime.Record
	ctx         *runtime.TaskContext
}

func (o *opBinlog) Close() error {
	o.server.Stop()
	close(o.binlogChan)
	return nil
}

func (o *opBinlog) Prepare(_ *runtime.TaskContext) (runtime.TaskRescUnit, error) {
	o.binlogChan = make(chan *runtime.Record, 1024)
	if test.GetGlobalTestFlag() {
		server, err := mock.NewTestServer(o.addr, o)
		if err != nil {
			return nil, err
		}
		o.server = server
	} else {
		lis, err := net.Listen("tcp", o.addr)
		if err != nil {
			return nil, err
		}
		s := grpc.NewServer()
		o.server = s
		pb.RegisterTestServiceServer(s, o)
		go func() {
			err1 := s.Serve(lis)
			if err1 != nil {
				log.L().Logger.Error("start grpc server failed", zap.Error(err))
				panic(err1)
			}
		}()
	}
	return runtime.NewSimpleTRU(model.Benchmark), nil
}

func (o *opBinlog) NextWantedInputIdx() int {
	if o.cacheRecord != nil {
		return runtime.DontNeedData
	}
	return runtime.DontRequireIndex
}

func (o *opBinlog) Next(ctx *runtime.TaskContext, r *runtime.Record, _ int) ([]runtime.Chunk, bool, error) {
	if o.ctx == nil {
		o.ctx = ctx
	}
	if o.cacheRecord != nil {
		r = o.cacheRecord
	}
	select {
	case o.binlogChan <- r:
		o.cacheRecord = nil
		return nil, false, nil
	default:
		o.cacheRecord = r
		return nil, true, nil
	}
}

func (o *opBinlog) FeedBinlog(req *pb.TestBinlogRequest, server pb.TestService_FeedBinlogServer) error {
	id := int(req.Gtid)
	if id > len(o.wal) {
		return server.Send(&pb.Record{
			Err: &pb.Error{Message: fmt.Sprintf("invalid gtid %d", id)},
		})
	}
	for id < len(o.wal) {
		err := server.Send(o.wal[id].Payload.(*pb.Record))
		id++
		if err != nil {
			return err
		}
	}
	for record := range o.binlogChan {
		r := record.Payload.(*pb.Record)
		r.TimeTracer = append(r.TimeTracer, time.Now().UnixNano())
		o.wal = append(o.wal, record)
		err := server.Send(r)
		if err != nil {
			return err
		}
		id++
		o.ctx.Wake()
	}
	return nil
}
