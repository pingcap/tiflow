package producer

import (
	"fmt"
	"io"
	"sync"

	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/test/mock"
)

type record struct {
	pk        int32
	schemaVer int32
	tp        pb.Record_RecordType
	tid       int32
	gtid      int32
}

func (r *record) toPB() *pb.Record {
	return &pb.Record{
		Tp:        r.tp,
		SchemaVer: r.schemaVer,
		Tid:       r.tid,
		Gtid:      r.gtid,
		Pk:        r.pk,
	}
}

type tableProducer struct {
	tid        int32
	pk         int32
	schemaVer  int32
	binlogChan []chan record
	dataCnt    int32
}

func (t *tableProducer) run() {
	binlogID := 0
	for {
		t.pk++
		if t.pk%1000 == 0 {
			t.schemaVer++
			ddlLock.Lock()
			for _, ch := range t.binlogChan {
				ch <- record{
					tp:        pb.Record_DDL,
					tid:       t.tid,
					schemaVer: t.schemaVer,
				}
			}
			ddlLock.Unlock()
		}
		ch := t.binlogChan[binlogID]
		ch <- record{
			tp:  pb.Record_Data,
			tid: t.tid,
			pk:  t.pk,
		}
		binlogID = (binlogID + 1) % len(t.binlogChan)
		if t.pk == t.dataCnt {
			return
		}
	}
}

var ddlLock = new(sync.Mutex)

type BinlogTestServer struct {
	binlogChan chan record
	wal        []record
	addr       string
}

func (b *BinlogTestServer) FeedBinlog(req *pb.TestBinlogRequest, server pb.TestService_FeedBinlogServer) error {
	id := int(req.Gtid)
	if id > len(b.wal) {
		return server.Send(&pb.Record{
			Err: &pb.Error{Message: fmt.Sprintf("invalid gtid %d", id)},
		})
	}
	for id < len(b.wal) {
		err := server.Send(b.wal[id].toPB())
		id++
		if err != nil {
			return err
		}
	}
	for record := range b.binlogChan {
		record.gtid = int32(len(b.wal))
		b.wal = append(b.wal, record)
		err := server.Send(b.wal[id].toPB())
		if err != nil {
			return err
		}
		id++
	}
	return io.EOF
}

func StartProducerForTest(addrs []string, tableNum int32, dataCnt int32) ([]mock.GrpcServer, error) {
	channels := make([]chan record, 0, len(addrs))
	servers := make([]mock.GrpcServer, 0, len(addrs))
	for _, addr := range addrs {
		server := &BinlogTestServer{
			binlogChan: make(chan record, 1024),
			addr:       addr,
		}
		channels = append(channels, server.binlogChan)

		grpcServer, err := mock.NewTestServer(addr, server)
		if err != nil {
			return nil, err
		}
		servers = append(servers, grpcServer)
	}
	for tid := int32(0); tid < tableNum; tid++ {
		producer := &tableProducer{
			tid:        tid,
			binlogChan: channels,
			dataCnt:    dataCnt,
		}
		go producer.run()
	}
	return servers, nil
}
