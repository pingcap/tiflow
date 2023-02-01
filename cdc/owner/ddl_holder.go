package owner

import (
	"github.com/pingcap/tiflow/cdc/model"
	"math"
)

type DDLHolder struct {
	pendingDDLs map[model.TableName][]*model.DDLEvent
	minDDLTs    model.Ts
	minDDLTable model.TableName
	minDDL      *model.DDLEvent
}

func NewDDLHolder() *DDLHolder {
	return &DDLHolder{
		pendingDDLs: make(map[model.TableName][]*model.DDLEvent),
		minDDLTs:    model.Ts(math.MaxUint64),
	}
}

func (h *DDLHolder) AddDDLs(ddls []*model.DDLEvent) {
	for _, d := range ddls {
		if tableDDLs, ok := h.pendingDDLs[d.TableInfo.TableName]; ok {
			tableDDLs = append(tableDDLs, d)
		} else {
			h.pendingDDLs[d.TableInfo.TableName] = []*model.DDLEvent{d}
		}
	}
}

func (h *DDLHolder) GetMinDDL() *model.DDLEvent {
	if h.minDDL == nil {
		for table, ddls := range h.pendingDDLs {
			if ddls[0].CommitTs < h.minDDLTs {
				h.minDDLTs = ddls[0].CommitTs
				h.minDDLTable = table
				h.minDDL = ddls[0]
			}
		}
	}
	return h.minDDL
}

func (h *DDLHolder) RemoveMinDDL() {
	minTableDDLs := h.pendingDDLs[h.minDDLTable]
	if len(minTableDDLs) == 1 {
		delete(h.pendingDDLs, h.minDDLTable)
	} else {
		h.pendingDDLs[h.minDDLTable] = minTableDDLs[1:]
	}
	h.minDDL = nil
}

func (h *DDLHolder) GetALLTableMinDDLTs() []model.TableBarrier {
	ret := make([]model.TableBarrier, 0, len(h.pendingDDLs))
	for table, ddls := range h.pendingDDLs {
		ret = append(ret, model.TableBarrier{
			BarrierTs: ddls[0].CommitTs,
			TableName: table,
		})
	}
	return ret
}
