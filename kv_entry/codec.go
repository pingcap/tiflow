package kv_entry

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/codec"
)

var (
	tablePrefix  = []byte{'t'}
	recordPrefix = []byte("_r")
	indexPrefix  = []byte("_i")
	metaPrefix   = []byte("m")
)

var (
	intLen            = 8
	tablePrefixLen    = len(tablePrefix)
	recordPrefixLen   = len(recordPrefix)
	metaPrefixLen     = len(metaPrefix)
	prefixTableIdLen  = tablePrefixLen + intLen  /*tableId*/
	prefixRecordIdLen = recordPrefixLen + intLen /*recordId*/
)

// MetaType is for data structure meta/data flag.
type MetaType byte

const (
	UnknownMetaType MetaType = 0
	// StringMeta is the flag for string meta.
	StringMeta MetaType = 'S'
	// StringData is the flag for string data.
	StringData MetaType = 's'
	// HashMeta is the flag for hash meta.
	HashMeta MetaType = 'H'
	// HashData is the flag for hash data.
	HashData MetaType = 'h'
	// ListMeta is the flag for list meta.
	ListMeta MetaType = 'L'
	// ListData is the flag for list data.
	ListData MetaType = 'l'
)

func hasTablePrefix(key []byte) bool {
	return key[0] == tablePrefix[0]
}

func hasRecordPrefix(key []byte) bool {
	return key[0] == recordPrefix[0] && key[1] == recordPrefix[1]
}

func hasIndexPrefix(key []byte) bool {
	return key[0] == indexPrefix[0] && key[1] == indexPrefix[1]
}
func hasMetaPrefix(key []byte) bool {
	return key[0] == metaPrefix[0]
}

func decodeTableId(key []byte) (odd []byte, tableId int64, err error) {
	if len(key) < prefixTableIdLen || !hasTablePrefix(key) {
		return nil, 0, errors.Errorf("invalid record key - %q", key)
	}
	key = key[tablePrefixLen:]
	odd, tableId, err = codec.DecodeInt(key)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return
}

func decodeRecordId(key []byte) (odd []byte, recordId int64, err error) {
	if len(key) < prefixRecordIdLen || !hasRecordPrefix(key) {
		return nil, 0, errors.Errorf("invalid record key - %q", key)
	}
	key = key[recordPrefixLen:]
	odd, recordId, err = codec.DecodeInt(key)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	return
}

func decodeMetaKey(ek []byte) (key string, tp MetaType, field []byte, err error) {
	if !hasMetaPrefix(ek) {
		return "", UnknownMetaType, nil, errors.New("invalid encoded hash data key prefix")
	}

	ek = ek[metaPrefixLen:]
	ek, rawKey, err := codec.DecodeBytes(ek, nil)
	if err != nil {
		return "", UnknownMetaType, nil, errors.Trace(err)
	}
	key = string(rawKey)

	ek, rawTp, err := codec.DecodeUint(ek)
	if err != nil {
		return "", UnknownMetaType, nil, errors.Trace(err)
	}
	tp = MetaType(rawTp)
	if len(ek) > 0 {
		ek, field, err = codec.DecodeBytes(ek, nil)
	}
	if len(ek) > 0 {
		// TODO: warning hash key decode failure
		panic("hash key decode failure, should never happen")
	}
	return
}

// DecodeIndexKey decodes the key and gets the tableID, indexID, indexValues.
//func DecodeIndexKey(key kv.Key) (tableID int64, indexID int64, indexValues []string, err error) {
//	k := key
//
//	tableID, indexID, isRecord, err := DecodeKeyHead(key)
//	if err != nil {
//		return 0, 0, nil, errors.Trace(err)
//	}
//	if isRecord {
//		return 0, 0, nil, errInvalidIndexKey.GenWithStack("invalid index key - %q", k)
//	}
//	key = key[prefixLen+intLen:]
//
//	for len(key) > 0 {
//		// FIXME: Without the schema information, we can only decode the raw kind of
//		// the column. For instance, MysqlTime is internally saved as uint64.
//		remain, d, e := codec.DecodeOne(key)
//		if e != nil {
//			return 0, 0, nil, errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, e)
//		}
//		str, e1 := d.ToString()
//		if e1 != nil {
//			return 0, 0, nil, errInvalidIndexKey.GenWithStack("invalid index key - %q %v", k, e1)
//		}
//		indexValues = append(indexValues, str)
//		key = remain
//	}
//	return
//}
