// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cvs

import (
	"context"
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/lib/registry"
	"github.com/pingcap/tiflow/engine/model"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

const (
	bufferSize = 1024
)

type strPair struct {
	firstStr  string
	secondStr string
}

// Config is cvs task config
type Config struct {
	Idx      int    `json:"Idx"`
	SrcHost  string `json:"SrcHost"`
	DstHost  string `json:"DstHost"`
	DstDir   string `json:"DstIdx"`
	StartLoc string `json:"StartLoc"`
}

// Status represents business status of cvs task
type Status struct {
	TaskConfig Config `json:"Config"`
	CurrentLoc string `json:"CurLoc"`
	Count      int64  `json:"Cnt"`
}

type connPool struct {
	sync.Mutex

	pool map[string]connArray
}

var pool connPool = connPool{pool: make(map[string]connArray)}

func (c *connPool) getConn(addr string) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()
	arr, ok := c.pool[addr]
	if !ok {
		for i := 0; i < 5; i++ {
			conn, err := grpc.Dial(addr, grpc.WithInsecure())
			if err != nil {
				return nil, err
			}
			arr = append(arr, conn)
		}
		c.pool[addr] = arr
	}
	i := rand.Intn(5)
	return arr[i], nil
}

type connArray []*grpc.ClientConn

type cvsTask struct {
	lib.BaseWorker
	Config
	counter  *atomic.Int64
	curLoc   string
	cancelFn func()
	buffer   chan strPair
	isEOF    bool

	statusCode struct {
		sync.RWMutex
		code libModel.WorkerStatusCode
	}
	runError struct {
		sync.RWMutex
		err error
	}

	statusRateLimiter *rate.Limiter
}

// RegisterWorker is used to register cvs task worker into global registry
func RegisterWorker() {
	constructor := func(ctx *dcontext.Context, id libModel.WorkerID, masterID libModel.MasterID, config lib.WorkerConfig) lib.WorkerImpl {
		return newCvsTask(ctx, id, masterID, config)
	}
	factory := registry.NewSimpleWorkerFactory(constructor, &Config{})
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(lib.CvsTask, factory)
}

func newCvsTask(ctx *dcontext.Context, _workerID libModel.WorkerID, masterID libModel.MasterID, conf lib.WorkerConfig) *cvsTask {
	cfg := conf.(*Config)
	task := &cvsTask{
		Config:            *cfg,
		curLoc:            cfg.StartLoc,
		buffer:            make(chan strPair, bufferSize),
		statusRateLimiter: rate.NewLimiter(rate.Every(time.Second), 1),
		counter:           atomic.NewInt64(0),
	}
	return task
}

// InitImpl implements WorkerImpl.InitImpl
func (task *cvsTask) InitImpl(ctx context.Context) error {
	log.L().Info("init the task  ", zap.Any("task id :", task.ID()))
	task.setStatusCode(libModel.WorkerStatusNormal)
	ctx, task.cancelFn = context.WithCancel(ctx)
	go func() {
		err := task.Receive(ctx)
		if err != nil {
			log.L().Error("error happened when reading data from the upstream ", zap.String("id", task.ID()), zap.Any("message", err.Error()))
			task.setRunError(err)
			task.setStatusCode(libModel.WorkerStatusError)
		}
	}()
	go func() {
		err := task.send(ctx)
		if err != nil {
			log.L().Error("error happened when writing data to the downstream ", zap.String("id", task.ID()), zap.Any("message", err.Error()))
			task.setRunError(err)
			task.setStatusCode(libModel.WorkerStatusError)
		} else {
			task.setStatusCode(libModel.WorkerStatusFinished)
		}
	}()

	return nil
}

// Tick is called on a fixed interval.
func (task *cvsTask) Tick(ctx context.Context) error {
	// log.L().Info("cvs task tick", zap.Any(" task id ", string(task.ID())+" -- "+strconv.FormatInt(task.counter, 10)))
	if task.statusRateLimiter.Allow() {
		err := task.BaseWorker.UpdateStatus(ctx, task.Status())
		if errors.ErrWorkerUpdateStatusTryAgain.Equal(err) {
			log.L().Warn("update status try again later", zap.String("id", task.ID()), zap.String("error", err.Error()))
			return nil
		}
		return err
	}
	switch task.getStatusCode() {
	case libModel.WorkerStatusFinished, libModel.WorkerStatusError, libModel.WorkerStatusStopped:
		return task.BaseWorker.Exit(ctx, task.Status(), task.getRunError())
	default:
	}
	return nil
}

// Status returns a short worker status to be periodically sent to the master.
func (task *cvsTask) Status() libModel.WorkerStatus {
	stats := &Status{
		TaskConfig: task.Config,
		CurrentLoc: task.curLoc,
		Count:      task.counter.Load(),
	}
	statsBytes, err := json.Marshal(stats)
	if err != nil {
		log.L().Panic("get stats error", zap.String("id", task.ID()), zap.Error(err))
	}
	return libModel.WorkerStatus{
		Code: task.getStatusCode(), ErrorMessage: "",
		ExtBytes: statsBytes,
	}
}

// Workload returns the current workload of the worker.
func (task *cvsTask) Workload() model.RescUnit {
	return 1
}

func (task *cvsTask) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	switch msg := message.(type) {
	case *libModel.StatusChangeRequest:
		switch msg.ExpectState {
		case libModel.WorkerStatusStopped:
			task.setStatusCode(libModel.WorkerStatusStopped)
		default:
			log.L().Info("FakeWorker: ignore status change state", zap.Int32("state", int32(msg.ExpectState)))
		}
	default:
		log.L().Info("unsupported message", zap.Any("message", message))
	}

	return nil
}

// CloseImpl tells the WorkerImpl to quitrunStatusWorker and release resources.
func (task *cvsTask) CloseImpl(ctx context.Context) error {
	if task.cancelFn != nil {
		task.cancelFn()
	}
	return nil
}

func (task *cvsTask) Receive(ctx context.Context) error {
	conn, err := pool.getConn(task.SrcHost)
	if err != nil {
		log.L().Error("cann't connect with the source address ", zap.String("id", task.ID()), zap.Any("message", task.SrcHost))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	reader, err := client.ReadLines(ctx, &pb.ReadLinesRequest{FileIdx: int32(task.Idx), LineNo: []byte(task.StartLoc)})
	if err != nil {
		log.L().Error("read data from file failed ", zap.String("id", task.ID()), zap.Error(err))
		return err
	}
	for {
		reply, err := reader.Recv()
		if err != nil {
			log.L().Error("read data failed", zap.String("id", task.ID()), zap.Error(err))
			if !task.isEOF {
				task.cancelFn()
			}
			return err
		}
		if reply.IsEof {
			log.L().Info("Reach the end of the file ", zap.String("id", task.ID()), zap.Any("fileID", task.Idx))
			close(task.buffer)
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case task.buffer <- strPair{firstStr: string(reply.Key), secondStr: string(reply.Val)}:
		}
		// waiting longer time to read lines slowly
	}
	return nil
}

func (task *cvsTask) send(ctx context.Context) error {
	conn, err := pool.getConn(task.DstHost)
	if err != nil {
		log.L().Error("can't connect with the destination address ", zap.Any("id", task.ID()), zap.Error(err))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	writer, err := client.WriteLines(ctx)
	if err != nil {
		log.L().Error("call write data rpc failed", zap.String("id", task.ID()), zap.Error(err))
		task.cancelFn()
		return err
	}
	for {
		select {
		case kv, more := <-task.buffer:
			if !more {
				log.L().Info("Reach the end of the file ", zap.String("id", task.ID()), zap.Any("cnt", task.counter.Load()), zap.String("last write", task.curLoc))
				resp, err := writer.CloseAndRecv()
				if err != nil {
					return err
				}
				if len(resp.ErrMsg) > 0 {
					log.L().Warn("close writing meet error", zap.String("id", task.ID()))
				}
				return nil
			}
			err := writer.Send(&pb.WriteLinesRequest{FileIdx: int32(task.Idx), Key: []byte(kv.firstStr), Value: []byte(kv.secondStr), Dir: task.DstDir})
			if err != nil {
				log.L().Error("call write data rpc failed ", zap.String("id", task.ID()), zap.Error(err))
				task.cancelFn()
				return err
			}
			task.counter.Add(1)
			task.curLoc = kv.firstStr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (task *cvsTask) getStatusCode() libModel.WorkerStatusCode {
	task.statusCode.RLock()
	defer task.statusCode.RUnlock()
	return task.statusCode.code
}

func (task *cvsTask) setStatusCode(status libModel.WorkerStatusCode) {
	task.statusCode.Lock()
	defer task.statusCode.Unlock()
	task.statusCode.code = status
}

func (task *cvsTask) getRunError() error {
	task.runError.RLock()
	defer task.runError.RUnlock()
	return task.runError.err
}

func (task *cvsTask) setRunError(err error) {
	task.runError.Lock()
	defer task.runError.Unlock()
	task.runError.err = err
}
