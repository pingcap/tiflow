package model

type OperatorType int32

type Operator []byte

const (
	TableReaderType OperatorType = iota
	HashType
	TableSinkType
	ProducerType
	BinlogType
	// Task of job master.
	JobMasterType
)

// benchmark operators
type TableReaderOp struct {
	FlowID string `json:"flow-id"`
	Addr   string `json:"address"`
}

type HashOp struct {
	TableID int32 `json:"id"`
}

type TableSinkOp struct {
	TableID int32  `json:"id"`
	File    string `json:"file"`
}

type ProducerOp struct {
	TableID      int32 `json:"tbl-num"`
	RecordCnt    int32 `json:"rcd-cnt"`
	DDLFrequency int32 `json:"ddl-freq"`
	OutputCnt    int   `json:"output-cnt"`
}

type BinlogOp struct {
	Address string `json:"addr"`
}
