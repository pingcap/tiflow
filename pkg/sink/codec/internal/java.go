// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.orglicensesLICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import "github.com/pingcap/tidb/pkg/parser/mysql"

// JavaSQLType is the sql type defined in class java.sql.Types in java sdk
type JavaSQLType int32

// jdk 1.8
const (
	JavaSQLTypeBIT           JavaSQLType = -7
	JavaSQLTypeTINYINT       JavaSQLType = -6
	JavaSQLTypeSMALLINT      JavaSQLType = 5
	JavaSQLTypeINTEGER       JavaSQLType = 4
	JavaSQLTypeBIGINT        JavaSQLType = -5
	JavaSQLTypeREAL          JavaSQLType = 7
	JavaSQLTypeDOUBLE        JavaSQLType = 8
	JavaSQLTypeDECIMAL       JavaSQLType = 3
	JavaSQLTypeCHAR          JavaSQLType = 1
	JavaSQLTypeVARCHAR       JavaSQLType = 12
	JavaSQLTypeDATE          JavaSQLType = 91
	JavaSQLTypeTIME          JavaSQLType = 92
	JavaSQLTypeTIMESTAMP     JavaSQLType = 93
	JavaSQLTypeBINARY        JavaSQLType = -2
	JavaSQLTypeVARBINARY     JavaSQLType = -3
	JavaSQLTypeLONGVARBINARY JavaSQLType = -4
	JavaSQLTypeNULL          JavaSQLType = 0
	JavaSQLTypeBLOB          JavaSQLType = 2004
	JavaSQLTypeCLOB          JavaSQLType = 2005

	// unused
	// JavaSQLTypeLONGVARCHAR             JavaSQLType = -1
	// JavaSQLTypeFLOAT                   JavaSQLType = 6
	// JavaSQLTypeNUMERIC                 JavaSQLType = 2
	// JavaSQLTypeOTHER                   JavaSQLType = 1111
	// JavaSQLTypeJAVA_OBJECT             JavaSQLType = 2000
	// JavaSQLTypeDISTINCT                JavaSQLType = 2001
	// JavaSQLTypeSTRUCT                  JavaSQLType = 2002
	// JavaSQLTypeARRAY                   JavaSQLType = 2003
	// JavaSQLTypeREF                     JavaSQLType = 2006
	// JavaSQLTypeDATALINK                JavaSQLType = 70
	// JavaSQLTypeBOOLEAN                 JavaSQLType = 16
	// JavaSQLTypeROWID                   JavaSQLType = -8
	// JavaSQLTypeNCHAR                   JavaSQLType = -15
	// JavaSQLTypeNVARCHAR                JavaSQLType = -9
	// JavaSQLTypeLONGNVARCHAR            JavaSQLType = -16
	// JavaSQLTypeNCLOB                   JavaSQLType = 2011
	// JavaSQLTypeSQLXML                  JavaSQLType = 2009
	// JavaSQLTypeREF_CURSOR              JavaSQLType = 2012
	// JavaSQLTypeTIME_WITH_TIMEZONE      JavaSQLType = 2013
	// JavaSQLTypeTIMESTAMP_WITH_TIMEZONE JavaSQLType = 2014
)

// MySQLType2JavaType converts the mysql protocol types to java sql types
// see https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogBuffer.java#L132-L269
func MySQLType2JavaType(mysqlType byte, isBinary bool) JavaSQLType {
	switch mysqlType {
	case mysql.TypeTiny:
		return JavaSQLTypeTINYINT

	case mysql.TypeShort:
		return JavaSQLTypeSMALLINT

	case mysql.TypeLong:
		return JavaSQLTypeINTEGER

	case mysql.TypeFloat:
		return JavaSQLTypeREAL

	case mysql.TypeDouble:
		return JavaSQLTypeDOUBLE

	case mysql.TypeNull:
		return JavaSQLTypeNULL

	case mysql.TypeNewDecimal:
		return JavaSQLTypeDECIMAL

	case mysql.TypeTimestamp, mysql.TypeDatetime:
		return JavaSQLTypeTIMESTAMP

	case mysql.TypeLonglong:
		return JavaSQLTypeBIGINT

	case mysql.TypeInt24:
		return JavaSQLTypeINTEGER

	case mysql.TypeDate, mysql.TypeNewDate:
		return JavaSQLTypeDATE

	case mysql.TypeDuration:
		return JavaSQLTypeTIME

	case mysql.TypeYear:
		return JavaSQLTypeVARCHAR

	case mysql.TypeEnum:
		return JavaSQLTypeINTEGER

	case mysql.TypeSet:
		return JavaSQLTypeBIT

	// Blob related is not identical to the official implementation, since we do not know `meta` at the moment.
	// see https://github.com/alibaba/canal/blob/b54bea5e3337c9597c427a53071d214ff04628d1/dbsync/src/main/java/com/taobao/tddl/dbsync/binlog/event/RowsLogBuffer.java#L222-L231
	// But this does not matter, they will be `JavaSQLTypeBlob` or `JavaSQLTypeClob` finally.
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
		if isBinary {
			return JavaSQLTypeBLOB
		}
		return JavaSQLTypeCLOB

	case mysql.TypeVarString, mysql.TypeVarchar:
		if isBinary {
			return JavaSQLTypeBLOB
		}
		return JavaSQLTypeVARCHAR

	case mysql.TypeString:
		if isBinary {
			return JavaSQLTypeBLOB
		}
		return JavaSQLTypeCHAR

	// Geometry is not supported, this should not hit.
	case mysql.TypeGeometry:
		return JavaSQLTypeBINARY

	case mysql.TypeBit:
		return JavaSQLTypeBIT

	case mysql.TypeJSON:
		return JavaSQLTypeVARCHAR

	case mysql.TypeTiDBVectorFloat32:
		return JavaSQLTypeVARCHAR

	default:
		return JavaSQLTypeVARCHAR
	}
}
