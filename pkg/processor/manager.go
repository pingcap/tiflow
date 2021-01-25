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
	Processors map[model.ChangeFeedID]*processor

	pdCli       pd.Client
	credential  *security.Credential
	captureInfo *model.CaptureInfo
}

func NewManager(pdCli pd.Client, credential *security.Credential, captureInfo *model.CaptureInfo) *Manager {
	return &Manager{
		Processors:  make(map[model.ChangeFeedID]*processor),
		pdCli:       pdCli,
		credential:  credential,
		captureInfo: captureInfo,
	}
}

func (m *Manager) Tick(ctx context.Context, state orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	log.Debug("LEOPPRO tick in processor manager", zap.Any("state", state))
	globalState := state.(*globalState)
	closeProcessor := func(changefeedID model.ChangeFeedID) {
		if processor, exist := m.Processors[changefeedID]; exist {
			err := processor.Close()
			if err != nil {
				log.Warn("failed to close processor", zap.Error(err))
			}
			delete(m.Processors, changefeedID)
		}
	}
	for changefeedID, changefeedState := range globalState.Changefeeds {
		if !changefeedState.Active() {
			closeProcessor(changefeedID)
			continue
		}
		processor, exist := m.Processors[changefeedID]
		if !exist {
			processor = NewProcessor(m.pdCli, m.credential, m.captureInfo)
			m.Processors[changefeedID] = processor
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

func (m *Manager) writeDebugInfo() {}
