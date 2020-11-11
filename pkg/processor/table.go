package processor

import (
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

type TablePipeline struct {
	localCheckpointTs model.Ts
	localResolvedTs   model.Ts
}

func (t *TablePipeline) LocalResolvedTs() model.Ts {

}

func (t *TablePipeline) LocalCheckpointTs() model.Ts {

}

func (t *TablePipeline) AsyncStop() (stopped bool, checkpointTs model.Ts) {

}

func NewTablePipeline(ctx pipeline.Context, tableID model.TableID, replicaInfo *model.TableReplicaInfo) *TablePipeline {
	p := pipeline.NewPipeline(ctx)

	return p
}
