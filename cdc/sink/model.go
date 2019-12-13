package sink

import (
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/ticdc/cdc/model"
)

// Message type
type MsgType byte

const (
	_ = iota
	// emit resolve type message.
	ResolveTsType MsgType = 1 + iota
	// txn message.
	TxnType
	// meta message.
	MetaType
)

type Message struct {
	MsgType MsgType
	// all cdc list
	CdcList   []string
	MetaCount int
	// resloveTs and txn message
	CdcID string
	// resloveTS type message
	ResloveTs uint64
	// txn type message
	Txn   *model.Txn
	Columns  map[string][]*timodel.ColumnInfo
}
