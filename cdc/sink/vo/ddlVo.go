package vo

type BatchDDLInfo struct{

	DDLList    []*DDLInfos
}

type DDLInfos struct{

	StartTimer int64
	CommitTimer int64
	DDLType  int32

	SchemaName string
	TableName string

	TableColumnNo  int32
	PreTableColumnNo  int32
	TableInfoList   []*ColVo

	PreTableInfoList   []*ColVo
	QuerySql string
}



type ColVo struct{

	ColumnType int
	ColumnName string

}
