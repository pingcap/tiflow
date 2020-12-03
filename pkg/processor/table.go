package processor

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

type TablePipeline struct {
	p *pipeline.Pipeline
}

func (t *TablePipeline) ResolvedTs() model.Ts {

}

func (t *TablePipeline) AsyncStop() {
	t.p.SendToFirstNode(pipeline.CommandMessage(&pipeline.Command{pipeline.CommandTypeShouldStop}))
}

func NewTablePipeline(ctx pipeline.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) *TablePipeline {
	p := pipeline.NewPipeline(ctx)

	return &TablePipeline{
		p: p,
	}
}
