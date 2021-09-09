package vo

type BatchDDLInfo struct{

	DDLList    []*DDLInfos
}

type DDLInfos struct{

	StartTimer int64
	CommitTimer int64
	SchemaName string
	TableName string
	TableColumnNo  int32
	PreTableColumnNo  int32
	DDLType    int32
	TableInfoList   []*ColVo
	PreTableInfoList   []*ColVo
	Query string
}



type ColVo struct{

	ColumnType byte
	ColumnName string

}
