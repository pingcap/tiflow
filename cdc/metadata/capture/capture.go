// Copyright 2023 PingCAP, Inc.
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

package capture

import (
	"sync"
	"time"

	"github.com/pingcap/tiflow/cdc/metadata"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/context"
)

type Capture struct {
    info *metadata.CaptureInfo
    captureObservation metadata.CaptureObservation
    controllerObservation meta.controllerObservation
    messageRouter messageRouter

    owners struct {
        sync.RWMutex
        m map[model.ChangeFeedID]owner
    }
    processors struct {
        sync.RWMutex
        m map[model.ChangeFeedID]processor
    }
    captures struct {
        sync.RWMutex
        m map[ID]*metadata.CaptureInfo
    }
    controller struct {
        sync.RWMutex
        // to create, update and stop a changefeed.
        // Sending to it is protected by a rwmutex.
        commands chan Command 
    }
}

// messageRouter is a hub of P2P messages. Capture, owner, processor will shares
// one same messageRouter object.
type messageRouter interface {
    Run(ctx context.Context) error
}

type owner struct {
    info *metadata.ChangefeedInfo
    changefeedObservation metadata.ChangefeedObservation
    messageRouter messageRouter
}

type processor struct {
    info *metadata.ChangefeedInfo
    changefeedObservation metadata.ChangefeedObservation
    messageRouter messageRouter
}

// 这个函数对应 cdc/capture/captureImpl.Run，会成为 Capture 的入口
func (c *Capture) Run(ctx context.Context) {
    go c.messageRouter.Run(ctx)
    go func() {
        for {
            if err := c.captureObservation.Heartbeat(); err != nil {
                return err
            }
            time.Sleep(100*time.Millisecond)
        }
    }()
    go func() {
        ownersUpdate := c.captureObservation.WatchOwners()
        processorsUpdate := c.captureObservation.WatchProcessors()
        for {
            select {
            case owners <-ownersUpdate:
                for owner := range c.owners.diff(owners) {
                    owner.Stop()
                    delete(c.owners, owner)
                    // So following messages to the owner can be skiped.
                    c.messageRouter.DeleteOwner(owner)
                }
                for owner := range owners.diff(c.owners) {
                    c.owners[owner.info.ChangefeedID] = owner
                    go owner.run()
                }
            case processors<-processorsUpdate:
                for processor := range c.processors.diff() {
                    // NOTE: Controller shouldn't remove processor with active tables,
                    // because it's very hard to avoid sink double write.
                    processor.Stop()
                }
            }
        }
    }()
    go func() {
        for {
            time.Sleep(time.Second)
            cfs = make([]*metadata.ChangefeedInfo, 0)
            progs = make([]*metadata.ChangefeedProgress, 0)
            for cf, owner := range c.owners {
                cfs = append(cfs, cf)
                var progress metadata.ChangefeedProgress = owner.GetProgress()
                progs = append(progs, progress)
            }
            if err := c.captureObservation.Advance(cfs, progs); err != nil {
                // handles InvalidOwner here.
            }
        }
    }()
    go func() {
        for {
            if !c.captureObservation.TryTakeControl() {
                time.Sleep(100*time.Millisecond)
            }
            if err = c.runController(ctx); err == metadata.InvalidController {
                continue
            }
        }
    }()
}

func (c *Capture) runController(ctx context.Context) error {
    go func() {
        for {
            c.controllerObservation.Heartbeat()
            time.Sleep(100*time.Millisecond)
        }
    }()

    for {
        capturesUpdate := c.captureObservation.WatchOwners()
        select {
        case captures = <-capturesUpdate:
            // If any captures are disconnected, remove owners and processors from them.
            // NOTE: here is a bad case: a capture is partitioned from metadata, but still
            // sinking to downstreams. It can never be handled correctly, so let it be.
            //
            // Maybe we can handle this case:
            // 1. if a capture is partitioned from metadata more than 10s, stop all processors;
            // 2. after a capture is partitioned from meta more than 20s, remove processors from the capture;
            // 3. so we have 10s to close all processors, in most case sink double write can be avoid.
        case command := <-c.controller.commands:
            handleCommand(command)
        }
    }
}
