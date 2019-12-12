package sink

import (
	"github.com/pingcap/ticdc/cdc/model"
	timodel "github.com/pingcap/parser/model"
)

// Message type
type MsgType byte
const (
	_ = iota
	// emit resolve type message.
	ResolveTsType MsgType = 1 + iota
	// txn message.
	TxnType
)

type Message struct {
	MsgType MsgType
	// all cdc list
	CdcList []string
	// resloveTs and txn message
	CdcID string
	// resloveTS type message
	ResloveTs int64
	// txn type message
	Txn   *model.Txn
	Columns  []*timodel.ColumnInfo
}
