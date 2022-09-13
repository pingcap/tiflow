// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import (
	"context"
	"sync"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/chann"
)

type encoderWorker struct {
	id           int
	changeFeedID model.ChangeFeedID
	wg           sync.WaitGroup
	encoder      codec.EventBatchEncoder
	writer       *dmlWriter
	errCh        chan<- error
}

func newWorker(
	workerID int,
	changefeedID model.ChangeFeedID,
	encoder codec.EventBatchEncoder,
	writer *dmlWriter,
	errCh chan<- error,
) *encoderWorker {
	return &encoderWorker{
		id:           workerID,
		changeFeedID: changefeedID,
		encoder:      encoder,
		writer:       writer,
		errCh:        errCh,
	}
}

func (w *encoderWorker) run(ctx context.Context, msgChan *chann.Chann[eventFragment]) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case frag := <-msgChan.Out():
				if frag.event == nil {
					w.writer.dispatchFragmentToWorker(frag)
					continue
				}
				err := w.encodeEvents(ctx, frag)
				if err != nil {
					w.errCh <- err
				}
			}
		}
	}()
}

func (w *encoderWorker) encodeEvents(ctx context.Context, frag eventFragment) error {
	for _, event := range frag.event.Event.Rows {
		err := w.encoder.AppendRowChangedEvent(ctx, "", event, nil)
		if err != nil {
			return err
		}
	}

	msgs := w.encoder.Build()
	frag.encodedMsgs = msgs
	w.writer.dispatchFragmentToWorker(frag)
	return nil
}

func (w *encoderWorker) stop() {
	w.wg.Wait()
}
