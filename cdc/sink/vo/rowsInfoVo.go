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
<<<<<<< HEAD
=======
	//CFlag  byte
	EventTypeValue int32
>>>>>>> 9a419b58b242d93bf1a26da2c9bc6bdd01e34df0
	ColumnList   []*ColumnVo

}

type ColumnVo struct{

	IsPkFlag bool
	ColumnType byte
	CFlag  byte
	ColumnValue string
	ColumnName string

}
