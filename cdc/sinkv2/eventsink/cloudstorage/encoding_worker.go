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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/chann"
	"go.uber.org/zap"
)

type encodingWorker struct {
	id           int
	changeFeedID model.ChangeFeedID
	wg           sync.WaitGroup
	encoder      codec.EventBatchEncoder
	writer       *dmlWriter
	errCh        chan<- error
}

func newEncodingWorker(
	workerID int,
	changefeedID model.ChangeFeedID,
	encoder codec.EventBatchEncoder,
	writer *dmlWriter,
	errCh chan<- error,
) *encodingWorker {
	return &encodingWorker{
		id:           workerID,
		changeFeedID: changefeedID,
		encoder:      encoder,
		writer:       writer,
		errCh:        errCh,
	}
}

func (w *encodingWorker) run(ctx context.Context, msgChan *chann.Chann[eventFragment]) {
	w.wg.Add(1)
	go func() {
		log.Debug("encoding worker started", zap.Int("id", w.id),
			zap.String("namespace", w.changeFeedID.Namespace),
			zap.String("changefeed", w.changeFeedID.ID))
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case frag := <-msgChan.Out():
				if frag.event == nil {
					w.writer.dispatchFragToDMLWorker(frag)
					continue
				}
				err := w.encodeEvents(ctx, frag)
				if err != nil {
					w.errCh <- err
					return
				}
			}
		}
	}()
}

func (w *encodingWorker) encodeEvents(ctx context.Context, frag eventFragment) error {
	for _, event := range frag.event.Event.Rows {
		err := w.encoder.AppendRowChangedEvent(ctx, "", event, nil)
		if err != nil {
			return err
		}
	}

	msgs := w.encoder.Build()
	frag.encodedMsgs = msgs
	w.writer.dispatchFragToDMLWorker(frag)
	return nil
}

func (w *encodingWorker) stop() {
	w.wg.Wait()
}
