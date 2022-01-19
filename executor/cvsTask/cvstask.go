package cvstask

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	BUFFERSIZE = 1024
)

type strPair struct {
	firstStr  string
	secondStr string
}

type cvsTask struct {
	*lib.BaseWorker
	srcAddr strPair
	dstAddr strPair
	counter int64
	status  lib.WorkerStatusCode
	buffer  chan strPair
}

func NewCvsTask(src strPair, dst strPair) lib.WorkerImpl {
	task := &cvsTask{}
	task.Impl = task
	task.srcAddr = src
	task.dstAddr = dst
	task.counter = 0
	task.buffer = make(chan strPair, BUFFERSIZE)
	return task
}

func (task *cvsTask) InitImpl(ctx context.Context) error {
	go func() {
		err := task.Receive(ctx)
		if err != nil {
			log.L().Info("error happened when reading data from the upstream ", zap.Any("message", err.Error()))
			task.status = lib.WorkerStatusError
		}
	}()
	go func() {
		err := task.Send(ctx)
		if err != nil {
			log.L().Info("error happened when writing data to the downstream ", zap.Any("message", err.Error()))
			task.status = lib.WorkerStatusError
		}
	}()
	return nil
}

// Tick is called on a fixed interval.
func (task *cvsTask) Tick(ctx context.Context) error {
	return nil
}

// Status returns a short worker status to be periodically sent to the master.
func (task *cvsTask) Status() (lib.WorkerStatus, error) {
	return lib.WorkerStatus{Code: lib.WorkerStatusNormal, ErrorMessage: "", Ext: task.counter}, nil
}

// Workload returns the current workload of the worker.
func (task *cvsTask) Workload() (model.RescUnit, error) {
	return 1, nil
}

// OnMasterFailover is called when the master is failed over.
func (task *cvsTask) OnMasterFailover(reason lib.MasterFailoverReason) error {
	return nil
}

// CloseImpl tells the WorkerImpl to quitrunStatusWorker and release resources.
func (task *cvsTask) CloseImpl() {
}

func (task *cvsTask) Receive(ctx context.Context) error {
	conn, err := grpc.Dial(task.srcAddr.firstStr, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the source address ", zap.Any("message", task.srcAddr.firstStr))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	defer conn.Close()
	reader, err := client.ReadLines(ctx, &pb.ReadLinesRequest{FileName: task.srcAddr.secondStr})
	if err != nil {
		log.L().Info("read data from file failed ", zap.Any("message", task.srcAddr.secondStr))
		return err
	}
	for {
		linestr, err := reader.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.L().Info("read data failed")
		}
		strs := strings.Split(linestr.Linestr, ",")
		if len(strs) < 2 {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case task.buffer <- strPair{firstStr: strs[0], secondStr: strs[1]}:
		}
	}
	return nil
}

func (task *cvsTask) Send(ctx context.Context) error {
	conn, err := grpc.Dial(task.dstAddr.firstStr, grpc.WithInsecure())
	if err != nil {
		log.L().Info("cann't connect with the destination address ", zap.Any("message", task.srcAddr.firstStr))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	defer conn.Close()
	writer, err := client.WriteLines(ctx)
	if err != nil {
		log.L().Info("call write data rpc failed ")
		return err
	}
	timeout := time.NewTimer(time.Second * 100)
	for {
		select {
		case <-ctx.Done():
			return nil
		case kv := <-task.buffer:
			err := writer.Send(&pb.WriteLinesRequest{FileName: task.dstAddr.secondStr, Key: kv.firstStr, Value: kv.secondStr})
			task.counter++
			if err != nil {
				log.L().Info("call write data rpc failed ")
			}
		case <-timeout.C:
			break
		default:
			time.Sleep(time.Second)
		}
	}
}
