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

package codec

import "github.com/pingcap/parser/mysql"

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
	JavaSQLTypeLONGVARCHAR   JavaSQLType = -1
	JavaSQLTypeDATE          JavaSQLType = 91
	JavaSQLTypeTIME          JavaSQLType = 92
	JavaSQLTypeTIMESTAMP     JavaSQLType = 93
	JavaSQLTypeBINARY        JavaSQLType = -2
	JavaSQLTypeVARBINARY     JavaSQLType = -3
	JavaSQLTypeLONGVARBINARY JavaSQLType = -4
	JavaSQLTypeNULL          JavaSQLType = 0
	JavaSQLTypeBLOB          JavaSQLType = 2004

	// unused
	// JavaSQLTypeFLOAT                   JavaSQLType = 6
	// JavaSQLTypeNUMERIC                 JavaSQLType = 2
	// JavaSQLTypeOTHER                   JavaSQLType = 1111
	// JavaSQLTypeJAVA_OBJECT             JavaSQLType = 2000
	// JavaSQLTypeDISTINCT                JavaSQLType = 2001
	// JavaSQLTypeSTRUCT                  JavaSQLType = 2002
	// JavaSQLTypeARRAY                   JavaSQLType = 2003
	// JavaSQLTypeCLOB                    JavaSQLType = 2005
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

// MysqlToJavaType converts the mysql protocol types to java sql types
func MysqlToJavaType(mysqlType byte) JavaSQLType {
	// see https://github.com/mysql/mysql-connector-j/blob/5.1.49/src/com/mysql/jdbc/MysqlDefs.java
	switch mysqlType {
	case mysql.TypeNewDecimal:
		return JavaSQLTypeDECIMAL

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

	case mysql.TypeTimestamp:
		return JavaSQLTypeTIMESTAMP

	case mysql.TypeLonglong:
		return JavaSQLTypeBIGINT

	case mysql.TypeInt24:
		return JavaSQLTypeINTEGER

	case mysql.TypeDate:
		return JavaSQLTypeDATE

	case mysql.TypeDuration:
		return JavaSQLTypeTIME

	case mysql.TypeDatetime:
		return JavaSQLTypeTIMESTAMP

	case mysql.TypeYear:
		return JavaSQLTypeDATE

	case mysql.TypeNewDate:
		return JavaSQLTypeDATE

	case mysql.TypeEnum:
		return JavaSQLTypeCHAR

	case mysql.TypeSet:
		return JavaSQLTypeCHAR

	case mysql.TypeTinyBlob:
		return JavaSQLTypeVARBINARY

	case mysql.TypeMediumBlob:
		return JavaSQLTypeLONGVARBINARY

	case mysql.TypeLongBlob:
		return JavaSQLTypeLONGVARBINARY

	case mysql.TypeBlob:
		return JavaSQLTypeLONGVARBINARY

	case mysql.TypeVarString, mysql.TypeVarchar:
		return JavaSQLTypeVARCHAR

	case mysql.TypeJSON:
		// json: see jdbc 8.0, https://github.com/mysql/mysql-connector-j/blob/8.0.20/src/main/core-api/java/com/mysql/cj/MysqlType.java
		return JavaSQLTypeLONGVARCHAR

	case mysql.TypeString:
		return JavaSQLTypeCHAR

	case mysql.TypeGeometry:
		return JavaSQLTypeBINARY

	case mysql.TypeBit:
		return JavaSQLTypeBIT

	default:
		return JavaSQLTypeVARCHAR
	}
}
