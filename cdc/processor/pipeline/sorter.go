// Copyright 2020 PingCAP, Inc.
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

package pipeline

import (
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	psorter "github.com/pingcap/ticdc/cdc/puller/sorter"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

type sorterNode struct {
	sortEngine model.SortEngine
	sortDir    string
	sorter     puller.EventSorter
	tableName  string // quoted schema and table, used in metircs only
	wg         errgroup.Group
}

func newSorterNode(sortEngine model.SortEngine, sortDir string, tableName string) pipeline.Node {
	return &sorterNode{
		sortEngine: sortEngine,
		sortDir:    sortDir,
		tableName:  tableName,
	}
}

func (n *sorterNode) Init(ctx pipeline.NodeContext) error {
	var sorter puller.EventSorter
	switch n.sortEngine {
	case model.SortInMemory:
		sorter = puller.NewEntrySorter()
	case model.SortInFile, model.SortUnified:
		err := util.IsDirAndWritable(n.sortDir)
		if err != nil {
			if os.IsNotExist(errors.Cause(err)) {
				err = os.MkdirAll(n.sortDir, 0o755)
				if err != nil {
					return errors.Annotate(cerror.WrapError(cerror.ErrProcessorSortDir, err), "create dir")
				}
			} else {
				return errors.Annotate(cerror.WrapError(cerror.ErrProcessorSortDir, err), "sort dir check")
			}
		}

		if n.sortEngine == model.SortInFile {
			sorter = puller.NewFileSorter(n.sortDir)
		} else {
			// Unified Sorter
			sorter = psorter.NewUnifiedSorter(n.sortDir, n.tableName, ctx.Vars().CaptureAddr)
		}
	default:
		return cerror.ErrUnknownSortEngine.GenWithStackByArgs(n.sortEngine)
	}
	n.wg.Go(func() error {
		ctx.Throw(errors.Trace(sorter.Run(ctx.StdContext())))
		return nil
	})
	n.wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case msg := <-sorter.Output():
				ctx.SendToNextNode(pipeline.PolymorphicEventMessage(msg))
			}
		}
	})
	n.sorter = sorter
	return nil
}

// Receive receives the message from the previous node
func (n *sorterNode) Receive(ctx pipeline.NodeContext) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		n.sorter.AddEntry(ctx.StdContext(), msg.PolymorphicEvent)
	default:
		ctx.SendToNextNode(msg)
	}
	return nil
}

func (n *sorterNode) Destroy(ctx pipeline.NodeContext) error {
	return n.wg.Wait()
}
