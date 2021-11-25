package model

type OperatorType int32

type Operator []byte

const (
	TableReaderType OperatorType = iota
	HashType
	TableSinkType
)

// benchmark operators
type TableReaderOp struct {
	FlowID   string `json:"flow-id"`
	Addr     string `json:"address"`
	TableNum int32  `json:"table-num"`
}

type HashOp struct {
	TableID int32 `json:"id"`
}

type TableSinkOp struct {
	TableID int32  `json:"id"`
	File    string `json:"file"`
}
