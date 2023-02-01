package owner

import (
	"github.com/pingcap/tiflow/cdc/model"
	"math"
)

// ddlHolder holds the pending DDL events and provides methods to
// access the minimum DDL event and the tableBarrier.
// Note: ddlHolder is not thread-safe. It is only used in the owner Tick loop.
type ddlHolder struct {
	// Note: DDLEvent(s) in the same table are ordered by commitTs.
	pendingDDLs    map[model.TableName][]*model.DDLEvent
	minDDLCommitTs model.Ts
	minDDL         *model.DDLEvent
}

// newDDLHolder creates and returns a new DDLHolder.
func newDDLHolder() *ddlHolder {
	return &ddlHolder{
		pendingDDLs:    make(map[model.TableName][]*model.DDLEvent),
		minDDLCommitTs: model.Ts(math.MaxUint64),
	}
}

// addDDL adds a list of DDLEvents to the ddlHolder.
func (h *ddlHolder) addDDL(events []*model.DDLEvent) {
	for _, e := range events {
		if tableDDLs, ok := h.pendingDDLs[e.TableInfo.TableName]; ok {
			h.pendingDDLs[e.TableInfo.TableName] = append(tableDDLs, e)
		} else {
			h.pendingDDLs[e.TableInfo.TableName] = []*model.DDLEvent{e}
		}
	}
}

// getMinDDL returns the DDLEvent with the smallest commit timestamp.
// It there is no DDLEvent in the ddlHolder, it returns nil.
func (h *ddlHolder) getMinDDL() *model.DDLEvent {
	if h.minDDL == nil {
		for _, ddls := range h.pendingDDLs {
			if ddls[0].CommitTs < h.minDDLCommitTs {
				h.minDDLCommitTs = ddls[0].CommitTs
				h.minDDL = ddls[0]
			}
		}
	}
	return h.minDDL
}

// removeMinDDL removes the DDLEvent with the smallest commit timestamp.
func (h *ddlHolder) removeMinDDL() {
	if h.minDDL == nil {
		return
	}

	minTableDDLs, ok := h.pendingDDLs[h.minDDL.TableInfo.TableName]
	if !ok {
		return
	}

	if len(minTableDDLs) == 1 {
		delete(h.pendingDDLs, h.minDDL.TableInfo.TableName)
	} else {
		h.pendingDDLs[h.minDDL.TableInfo.TableName] = minTableDDLs[1:]
	}

	h.minDDL = nil
}

// getAllTableBarrier returns the tableBarrier for all tables.
func (h *ddlHolder) getAllTableBarrier() []model.TableBarrier {
	ret := make([]model.TableBarrier, 0, len(h.pendingDDLs))
	for table, ddls := range h.pendingDDLs {
		ret = append(ret, model.TableBarrier{
			BarrierTs: ddls[0].CommitTs,
			TableName: table,
		})
	}
	return ret
}

// reset resets the ddlHolder.
func (h *ddlHolder) reset() {
	h.pendingDDLs = make(map[model.TableName][]*model.DDLEvent)
	h.minDDLCommitTs = model.Ts(math.MaxUint64)
	h.minDDL = nil
}
