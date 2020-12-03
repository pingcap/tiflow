package processor

import (
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller"
	psorter "github.com/pingcap/ticdc/cdc/puller/sorter"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/pingcap/ticdc/pkg/util"
)

type sorterNode struct {
	sortEngine model.SortEngine
	sortDir    string
	sorter     puller.EventSorter
}

func (n *sorterNode) Init(ctx pipeline.Context) error {
	var sorter puller.EventSorter
	switch n.sortEngine {
	case model.SortInMemory:
		sorter = puller.NewEntrySorter()
	case model.SortInFile, model.SortUnified:
		err := util.IsDirAndWritable(n.sortDir)
		if err != nil {
			if os.IsNotExist(errors.Cause(err)) {
				err = os.MkdirAll(n.sortDir, 0755)
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
			sorter = psorter.NewUnifiedSorter(n.sortDir, tableName, util.CaptureAddrFromCtx(ctx))
		}
	default:
		return cerror.ErrUnknownSortEngine.GenWithStackByArgs(n.sortEngine)
	}
	go ctx.Throw(sorter.Run(ctx))
	go func() {
		for {
			select {
			case <-ctx.Done():
			case msg := <-sorter.Output():
				ctx.SendToNextNode(pipeline.PolymorphicEventMessage(msg))
			}
		}
	}()
	n.sorter = sorter
	return nil
}

// Receive receives the message from the previous node
func (n *sorterNode) Receive(ctx pipeline.Context) error {
	msg := ctx.Message()
	switch msg.Tp {
	case pipeline.MessageTypePolymorphicEvent:
		n.sorter.AddEntry(ctx, msg.PolymorphicEvent)
	default:
		ctx.SendToNextNode(msg)
	}
	return nil
}

func (n *sorterNode) Destroy(ctx pipeline.Context) error {
	// TODO: check is the sorter should be destroyed
	return nil
}
