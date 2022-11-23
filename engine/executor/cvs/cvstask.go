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

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/registry"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
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
	framework.BaseWorker
	Config
	counter  *atomic.Int64
	curLoc   string
	cancelFn func()
	buffer   chan strPair
	isEOF    bool

	statusCode struct {
		sync.RWMutex
		code frameModel.WorkerState
	}
	runError struct {
		sync.RWMutex
		err error
	}

	statusRateLimiter *rate.Limiter
}

// RegisterWorker is used to register cvs task worker into global registry
func RegisterWorker() {
	factory := registry.NewSimpleWorkerFactory(newCvsTask)
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(frameModel.CvsTask, factory)
}

func newCvsTask(ctx *dcontext.Context, _workerID frameModel.WorkerID, masterID frameModel.MasterID, conf *Config) *cvsTask {
	task := &cvsTask{
		Config:            *conf,
		curLoc:            conf.StartLoc,
		buffer:            make(chan strPair, bufferSize),
		statusRateLimiter: rate.NewLimiter(rate.Every(time.Second), 1),
		counter:           atomic.NewInt64(0),
	}
	return task
}

// InitImpl implements WorkerImpl.InitImpl
func (task *cvsTask) InitImpl(ctx context.Context) error {
	log.Info("init the task  ", zap.Any("task id :", task.ID()))
	task.setState(frameModel.WorkerStateNormal)
	// Don't use the ctx from the caller. Caller may cancel the ctx after InitImpl returns.
	ctx, task.cancelFn = context.WithCancel(context.Background())
	go func() {
		err := task.Receive(ctx)
		if err != nil {
			log.Error("error happened when reading data from the upstream ", zap.String("id", task.ID()), zap.Any("message", err.Error()))
			task.setRunError(err)
			task.setState(frameModel.WorkerStateError)
		}
	}()
	go func() {
		err := task.send(ctx)
		if err != nil {
			log.Error("error happened when writing data to the downstream ", zap.String("id", task.ID()), zap.Any("message", err.Error()))
			task.setRunError(err)
			task.setState(frameModel.WorkerStateError)
		} else {
			task.setState(frameModel.WorkerStateFinished)
		}
	}()

	return nil
}

// Tick is called on a fixed interval.
func (task *cvsTask) Tick(ctx context.Context) error {
	// log.Info("cvs task tick", zap.Any(" task id ", string(task.ID())+" -- "+strconv.FormatInt(task.counter, 10)))
	if task.statusRateLimiter.Allow() {
		err := task.BaseWorker.UpdateStatus(ctx, task.Status())
		if errors.Is(err, errors.ErrWorkerUpdateStatusTryAgain) {
			log.Warn("update status try again later", zap.String("id", task.ID()), zap.String("error", err.Error()))
			return nil
		}
		return err
	}

	exitReason := framework.ExitReasonUnknown
	switch task.getState() {
	case frameModel.WorkerStateFinished:
		exitReason = framework.ExitReasonFinished
	case frameModel.WorkerStateError:
		exitReason = framework.ExitReasonFailed
	case frameModel.WorkerStateStopped:
		exitReason = framework.ExitReasonCanceled
	default:
	}

	if exitReason == framework.ExitReasonUnknown {
		return nil
	}

	return task.BaseWorker.Exit(ctx, exitReason, task.getRunError(), task.Status().ExtBytes)
}

// Status returns a short worker status to be periodically sent to the master.
func (task *cvsTask) Status() frameModel.WorkerStatus {
	stats := &Status{
		TaskConfig: task.Config,
		CurrentLoc: task.curLoc,
		Count:      task.counter.Load(),
	}
	statsBytes, err := json.Marshal(stats)
	if err != nil {
		log.Panic("get stats error", zap.String("id", task.ID()), zap.Error(err))
	}
	return frameModel.WorkerStatus{
		State:    task.getState(),
		ErrorMsg: "",
		ExtBytes: statsBytes,
	}
}

func (task *cvsTask) OnMasterMessage(ctx context.Context, topic p2p.Topic, message p2p.MessageValue) error {
	switch msg := message.(type) {
	case *frameModel.StatusChangeRequest:
		switch msg.ExpectState {
		case frameModel.WorkerStateStopped:
			task.setState(frameModel.WorkerStateStopped)
		default:
			log.Info("FakeWorker: ignore status change state", zap.Int32("state", int32(msg.ExpectState)))
		}
	default:
		log.Info("unsupported message", zap.Any("message", message))
	}

	return nil
}

// CloseImpl tells the WorkerImpl to quitrunStatusWorker and release resources.
func (task *cvsTask) CloseImpl(ctx context.Context) {
	if task.cancelFn != nil {
		task.cancelFn()
	}
}

func (task *cvsTask) Receive(ctx context.Context) error {
	conn, err := pool.getConn(task.SrcHost)
	if err != nil {
		log.Error("cann't connect with the source address ", zap.String("id", task.ID()), zap.Any("message", task.SrcHost))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	reader, err := client.ReadLines(ctx, &pb.ReadLinesRequest{FileIdx: int32(task.Idx), LineNo: []byte(task.StartLoc)})
	if err != nil {
		log.Error("read data from file failed ", zap.String("id", task.ID()), zap.Error(err))
		return err
	}
	for {
		reply, err := reader.Recv()
		if err != nil {
			log.Error("read data failed", zap.String("id", task.ID()), zap.Error(err))
			if !task.isEOF {
				task.cancelFn()
			}
			return err
		}
		if reply.IsEof {
			log.Info("Reach the end of the file ", zap.String("id", task.ID()), zap.Any("fileID", task.Idx))
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
		log.Error("can't connect with the destination address ", zap.Any("id", task.ID()), zap.Error(err))
		return err
	}
	client := pb.NewDataRWServiceClient(conn)
	writer, err := client.WriteLines(ctx)
	if err != nil {
		log.Error("call write data rpc failed", zap.String("id", task.ID()), zap.Error(err))
		task.cancelFn()
		return err
	}
	for {
		select {
		case kv, more := <-task.buffer:
			if !more {
				log.Info("Reach the end of the file ", zap.String("id", task.ID()), zap.Any("cnt", task.counter.Load()), zap.String("last write", task.curLoc))
				resp, err := writer.CloseAndRecv()
				if err != nil {
					return err
				}
				if len(resp.ErrMsg) > 0 {
					log.Warn("close writing meet error", zap.String("id", task.ID()))
				}
				return nil
			}
			err := writer.Send(&pb.WriteLinesRequest{FileIdx: int32(task.Idx), Key: []byte(kv.firstStr), Value: []byte(kv.secondStr), Dir: task.DstDir})
			if err != nil {
				log.Error("call write data rpc failed ", zap.String("id", task.ID()), zap.Error(err))
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

func (task *cvsTask) getState() frameModel.WorkerState {
	task.statusCode.RLock()
	defer task.statusCode.RUnlock()
	return task.statusCode.code
}

func (task *cvsTask) setState(status frameModel.WorkerState) {
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
