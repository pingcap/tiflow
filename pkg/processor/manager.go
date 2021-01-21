package processor

import (
	"context"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/security"
	pd "github.com/tikv/pd/client"
)

type Manager struct {
	processors map[model.ChangeFeedID]*processor

	pdCli       pd.Client
	credential  *security.Credential
	captureInfo model.CaptureInfo
}

func (m *Manager) Tick(ctx context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	globalState := state.(*globalState)
	closeProcessor := func(changefeedID model.ChangeFeedID) {
		if processor, exist := m.processors[changefeedID]; exist {
			err := processor.Close()
			if err != nil {
				log.Warn("failed to close processor", zap.Error(err))
			}
			delete(m.processors, changefeedID)
		}
	}
	for changefeedID, changefeedState := range globalState.Changefeeds {
		if !changefeedState.Active() {
			closeProcessor(changefeedID)
		}
		processor, exist := m.processors[changefeedID]
		if !exist {
			processor = NewProcessor(m.pdCli, m.credential, m.captureInfo)
			m.processors[changefeedID] = processor
		}
		if _, err := processor.Tick(ctx, changefeedState); err != nil {
			if cerrors.ErrReactorFinished.Equal(err) {
				closeProcessor(changefeedID)
				continue
			}
			return state, errors.Trace(err)
		}
	}
	for _, changefeedID := range globalState.GetRemovedChangefeeds() {
		closeProcessor(changefeedID)
	}
	return state, nil
}
