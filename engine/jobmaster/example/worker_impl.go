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

package example

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

var _ framework.Worker = &exampleWorker{}

var (
	tickKey   = "tick_count"
	testTopic = "test_topic"
	testMsg   = "test_msg"
)

type exampleWorker struct {
	framework.BaseWorker

	work struct {
		mu        sync.Mutex
		tickCount int
		finished  bool
	}
	wg sync.WaitGroup
}

func (w *exampleWorker) run() {
	defer w.wg.Done()

	time.Sleep(time.Second)
	w.work.mu.Lock()
	count := w.work.tickCount
	w.work.mu.Unlock()
	// nolint:errcheck
	_, _ = w.BaseWorker.MetaKVClient().Put(
		context.TODO(), tickKey, strconv.Itoa(count))

	w.work.mu.Lock()
	w.work.finished = true
	w.work.mu.Unlock()

	// nolint:errcheck
	_ = w.BaseWorker.SendMessage(context.TODO(), testTopic, testMsg, true)
}

func (w *exampleWorker) InitImpl(ctx context.Context) error {
	log.L().Info("InitImpl")
	w.wg.Add(1)
	go w.run()
	return nil
}

func (w *exampleWorker) Tick(ctx context.Context) error {
	log.L().Info("Tick")
	w.work.mu.Lock()
	w.work.tickCount++
	count := w.work.tickCount
	w.work.mu.Unlock()

	storage, err := w.OpenStorage(nil, "/local/example")
	if err != nil {
		return err
	}

	file, err := storage.BrExternalStorage().Create(ctx, strconv.Itoa(count)+".txt")
	if err != nil {
		return err
	}
	_, err = file.Write(ctx, []byte(strconv.Itoa(count)))
	if err != nil {
		return err
	}

	if err := file.Close(ctx); err != nil {
		return err
	}
	return storage.Persist(ctx)
}

func (w *exampleWorker) Status() frameModel.WorkerStatus {
	log.L().Info("Status")
	code := frameModel.WorkerStatusNormal
	w.work.mu.Lock()
	finished := w.work.finished
	w.work.mu.Unlock()

	if finished {
		code = frameModel.WorkerStatusFinished
	}
	return frameModel.WorkerStatus{Code: code}
}

func (w *exampleWorker) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("OnMasterMessage", zap.Any("message", message))
	return nil
}

func (w *exampleWorker) CloseImpl(ctx context.Context) error {
	log.L().Info("CloseImpl")
	w.wg.Wait()
	return nil
}
