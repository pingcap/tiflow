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

type JavaSqlType int32

const (
	// jdk 1.8
	JavaSqlTypeBIT           JavaSqlType = -7
	JavaSqlTypeTINYINT       JavaSqlType = -6
	JavaSqlTypeSMALLINT      JavaSqlType = 5
	JavaSqlTypeINTEGER       JavaSqlType = 4
	JavaSqlTypeBIGINT        JavaSqlType = -5
	JavaSqlTypeREAL          JavaSqlType = 7
	JavaSqlTypeDOUBLE        JavaSqlType = 8
	JavaSqlTypeDECIMAL       JavaSqlType = 3
	JavaSqlTypeCHAR          JavaSqlType = 1
	JavaSqlTypeVARCHAR       JavaSqlType = 12
	JavaSqlTypeLONGVARCHAR   JavaSqlType = -1
	JavaSqlTypeDATE          JavaSqlType = 91
	JavaSqlTypeTIME          JavaSqlType = 92
	JavaSqlTypeTIMESTAMP     JavaSqlType = 93
	JavaSqlTypeBINARY        JavaSqlType = -2
	JavaSqlTypeVARBINARY     JavaSqlType = -3
	JavaSqlTypeLONGVARBINARY JavaSqlType = -4
	JavaSqlTypeNULL          JavaSqlType = 0
	JavaSqlTypeBLOB          JavaSqlType = 2004

	// unused
	//JavaSqlTypeFLOAT                   JavaSqlType = 6
	//JavaSqlTypeNUMERIC                 JavaSqlType = 2
	//JavaSqlTypeOTHER                   JavaSqlType = 1111
	//JavaSqlTypeJAVA_OBJECT             JavaSqlType = 2000
	//JavaSqlTypeDISTINCT                JavaSqlType = 2001
	//JavaSqlTypeSTRUCT                  JavaSqlType = 2002
	//JavaSqlTypeARRAY                   JavaSqlType = 2003
	//JavaSqlTypeCLOB                    JavaSqlType = 2005
	//JavaSqlTypeREF                     JavaSqlType = 2006
	//JavaSqlTypeDATALINK                JavaSqlType = 70
	//JavaSqlTypeBOOLEAN                 JavaSqlType = 16
	//JavaSqlTypeROWID                   JavaSqlType = -8
	//JavaSqlTypeNCHAR                   JavaSqlType = -15
	//JavaSqlTypeNVARCHAR                JavaSqlType = -9
	//JavaSqlTypeLONGNVARCHAR            JavaSqlType = -16
	//JavaSqlTypeNCLOB                   JavaSqlType = 2011
	//JavaSqlTypeSQLXML                  JavaSqlType = 2009
	//JavaSqlTypeREF_CURSOR              JavaSqlType = 2012
	//JavaSqlTypeTIME_WITH_TIMEZONE      JavaSqlType = 2013
	//JavaSqlTypeTIMESTAMP_WITH_TIMEZONE JavaSqlType = 2014
)

func MysqlToJavaType(mysqlType byte) JavaSqlType {
	// see https://github.com/mysql/mysql-connector-j/blob/5.1.49/src/com/mysql/jdbc/MysqlDefs.java
	switch mysqlType {
	case mysql.TypeNewDecimal, mysql.TypeDecimal:
		return JavaSqlTypeDECIMAL

	case mysql.TypeTiny:
		return JavaSqlTypeTINYINT

	case mysql.TypeShort:
		return JavaSqlTypeSMALLINT

	case mysql.TypeLong:
		return JavaSqlTypeINTEGER

	case mysql.TypeFloat:
		return JavaSqlTypeREAL

	case mysql.TypeDouble:
		return JavaSqlTypeDOUBLE

	case mysql.TypeNull:
		return JavaSqlTypeNULL

	case mysql.TypeTimestamp:
		return JavaSqlTypeTIMESTAMP

	case mysql.TypeLonglong:
		return JavaSqlTypeBIGINT

	case mysql.TypeInt24:
		return JavaSqlTypeINTEGER

	case mysql.TypeDate:
		return JavaSqlTypeDATE

	case mysql.TypeDuration:
		return JavaSqlTypeTIME

	case mysql.TypeDatetime:
		return JavaSqlTypeTIMESTAMP

	case mysql.TypeYear:
		return JavaSqlTypeDATE

	case mysql.TypeNewDate:
		return JavaSqlTypeDATE

	case mysql.TypeEnum:
		return JavaSqlTypeCHAR

	case mysql.TypeSet:
		return JavaSqlTypeCHAR

	case mysql.TypeTinyBlob:
		return JavaSqlTypeVARBINARY

	case mysql.TypeMediumBlob:
		return JavaSqlTypeLONGVARBINARY

	case mysql.TypeLongBlob:
		return JavaSqlTypeLONGVARBINARY

	case mysql.TypeBlob:
		return JavaSqlTypeLONGVARBINARY

	case mysql.TypeVarString, mysql.TypeVarchar:
		return JavaSqlTypeVARCHAR

	case mysql.TypeJSON:
		// json: see jdbc 8.0, https://github.com/mysql/mysql-connector-j/blob/8.0.20/src/main/core-api/java/com/mysql/cj/MysqlType.java
		return JavaSqlTypeLONGVARCHAR

	case mysql.TypeString:
		return JavaSqlTypeCHAR

	case mysql.TypeGeometry:
		return JavaSqlTypeBINARY

	case mysql.TypeBit:
		return JavaSqlTypeBIT

	default:
		return JavaSqlTypeVARCHAR
	}
}
