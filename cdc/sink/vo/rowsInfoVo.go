package vo

type BatchRowsInfo struct{

	RowList    []*RowInfos
}

type RowInfos struct{

	StartTimer int64
	CommitTimer int64
	RowID  int64
	ColumnNo  int32
	OperType  int32
	SchemaName string
	TableName string
	//CFlag  byte
	EventTypeValue int32
	ColumnList   []*ColumnVo

}

type ColumnVo struct{

	IsPkFlag bool
	ColumnType byte
	CFlag  byte
	ColumnValue string
	ColumnName string

}
