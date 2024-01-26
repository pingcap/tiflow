// Code generated by github.com/actgardner/gogen-avro/v10. DO NOT EDIT.
/*
 * SOURCE:
 *     schema.avsc
 */
package avro

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/actgardner/gogen-avro/v10/compiler"
	"github.com/actgardner/gogen-avro/v10/vm"
	"github.com/actgardner/gogen-avro/v10/vm/types"
)

var _ = fmt.Printf

type Message struct {
	Payload UnionWatermarkBootstrapDDLDML `json:"payload"`
}

const MessageAvroCRC64Fingerprint = "\xe49\xd9\xff\x83\xc5p,"

func NewMessage() Message {
	r := Message{}
	r.Payload = NewUnionWatermarkBootstrapDDLDML()

	return r
}

func DeserializeMessage(r io.Reader) (Message, error) {
	t := NewMessage()
	deser, err := compiler.CompileSchemaBytes([]byte(t.Schema()), []byte(t.Schema()))
	if err != nil {
		return t, err
	}

	err = vm.Eval(r, deser, &t)
	return t, err
}

func DeserializeMessageFromSchema(r io.Reader, schema string) (Message, error) {
	t := NewMessage()

	deser, err := compiler.CompileSchemaBytes([]byte(schema), []byte(t.Schema()))
	if err != nil {
		return t, err
	}

	err = vm.Eval(r, deser, &t)
	return t, err
}

func writeMessage(r Message, w io.Writer) error {
	var err error
	err = writeUnionWatermarkBootstrapDDLDML(r.Payload, w)
	if err != nil {
		return err
	}
	return err
}

func (r Message) Serialize(w io.Writer) error {
	return writeMessage(r, w)
}

func (r Message) Schema() string {
	return "{\"docs\":\"the wrapper for all kind of messages\",\"fields\":[{\"name\":\"payload\",\"type\":[{\"docs\":\"the message format of the watermark event\",\"fields\":[{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"commitTs\",\"type\":\"long\"},{\"name\":\"buildTs\",\"type\":\"long\"}],\"name\":\"Watermark\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"},{\"docs\":\"the message format of the bootstrap event\",\"fields\":[{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"buildTs\",\"type\":\"long\"},{\"name\":\"tableSchema\",\"type\":{\"docs\":\"table schema information\",\"fields\":[{\"name\":\"database\",\"type\":\"string\"},{\"name\":\"table\",\"type\":\"string\"},{\"name\":\"tableID\",\"type\":\"long\"},{\"name\":\"version\",\"type\":\"long\"},{\"name\":\"columns\",\"type\":{\"items\":{\"docs\":\"each column's schema information\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"dataType\",\"type\":{\"docs\":\"each column's mysql type information\",\"fields\":[{\"name\":\"mysqlType\",\"type\":\"string\"},{\"name\":\"charset\",\"type\":\"string\"},{\"name\":\"collate\",\"type\":\"string\"},{\"name\":\"length\",\"type\":\"long\"},{\"default\":null,\"name\":\"decimal\",\"type\":[\"null\",\"int\"]},{\"default\":null,\"name\":\"elements\",\"type\":[\"null\",{\"items\":\"string\",\"type\":\"array\"}]},{\"default\":null,\"name\":\"unsigned\",\"type\":[\"null\",\"boolean\"]},{\"default\":null,\"name\":\"zerofill\",\"type\":[\"null\",\"boolean\"]}],\"name\":\"DataType\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"}},{\"name\":\"nullable\",\"type\":\"boolean\"},{\"name\":\"default\",\"type\":[\"null\",\"string\"]}],\"name\":\"ColumnSchema\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"},\"type\":\"array\"}},{\"name\":\"indexes\",\"type\":{\"items\":{\"docs\":\"each index's schema information\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"unique\",\"type\":\"boolean\"},{\"name\":\"primary\",\"type\":\"boolean\"},{\"name\":\"nullable\",\"type\":\"boolean\"},{\"name\":\"columns\",\"type\":{\"items\":\"string\",\"type\":\"array\"}}],\"name\":\"IndexSchema\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"},\"type\":\"array\"}}],\"name\":\"TableSchema\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"}}],\"name\":\"Bootstrap\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"},{\"docs\":\"the message format of the DDL event\",\"fields\":[{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"type\",\"type\":{\"name\":\"DDLType\",\"symbols\":[\"CREATE\",\"ALTER\",\"ERASE\",\"RENAME\",\"TRUNCATE\",\"CINDEX\",\"DINDEX\",\"QUERY\"],\"type\":\"enum\"}},{\"name\":\"sql\",\"type\":\"string\"},{\"name\":\"commitTs\",\"type\":\"long\"},{\"name\":\"buildTs\",\"type\":\"long\"},{\"default\":null,\"name\":\"tableSchema\",\"type\":[\"null\",\"com.pingcap.simple.avro.TableSchema\"]},{\"default\":null,\"name\":\"preTableSchema\",\"type\":[\"null\",\"com.pingcap.simple.avro.TableSchema\"]}],\"name\":\"DDL\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"},{\"docs\":\"the message format of the DML event\",\"fields\":[{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"database\",\"type\":\"string\"},{\"name\":\"table\",\"type\":\"string\"},{\"name\":\"tableID\",\"type\":\"long\"},{\"name\":\"type\",\"type\":{\"name\":\"DMLType\",\"symbols\":[\"INSERT\",\"UPDATE\",\"DELETE\"],\"type\":\"enum\"}},{\"name\":\"commitTs\",\"type\":\"long\"},{\"name\":\"buildTs\",\"type\":\"long\"},{\"name\":\"schemaVersion\",\"type\":\"long\"},{\"default\":null,\"name\":\"claimCheckLocation\",\"type\":[\"null\",\"string\"]},{\"default\":null,\"name\":\"handleKeyOnly\",\"type\":[\"null\",\"boolean\"]},{\"default\":null,\"name\":\"checksum\",\"type\":[\"null\",{\"docs\":\"event's e2e checksum information\",\"fields\":[{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"corrupted\",\"type\":\"boolean\"},{\"name\":\"current\",\"type\":\"long\"},{\"name\":\"previous\",\"type\":\"long\"}],\"name\":\"Checksum\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"}]},{\"default\":null,\"name\":\"data\",\"type\":[\"null\",{\"default\":null,\"type\":\"map\",\"values\":[\"null\",\"long\",\"float\",\"double\",\"string\",\"bytes\"]}]},{\"default\":null,\"name\":\"old\",\"type\":[\"null\",{\"default\":null,\"type\":\"map\",\"values\":[\"null\",\"long\",\"float\",\"double\",\"string\",\"bytes\"]}]}],\"name\":\"DML\",\"namespace\":\"com.pingcap.simple.avro\",\"type\":\"record\"}]}],\"name\":\"com.pingcap.simple.avro.Message\",\"type\":\"record\"}"
}

func (r Message) SchemaName() string {
	return "com.pingcap.simple.avro.Message"
}

func (_ Message) SetBoolean(v bool)    { panic("Unsupported operation") }
func (_ Message) SetInt(v int32)       { panic("Unsupported operation") }
func (_ Message) SetLong(v int64)      { panic("Unsupported operation") }
func (_ Message) SetFloat(v float32)   { panic("Unsupported operation") }
func (_ Message) SetDouble(v float64)  { panic("Unsupported operation") }
func (_ Message) SetBytes(v []byte)    { panic("Unsupported operation") }
func (_ Message) SetString(v string)   { panic("Unsupported operation") }
func (_ Message) SetUnionElem(v int64) { panic("Unsupported operation") }

func (r *Message) Get(i int) types.Field {
	switch i {
	case 0:
		r.Payload = NewUnionWatermarkBootstrapDDLDML()

		w := types.Record{Target: &r.Payload}

		return w

	}
	panic("Unknown field index")
}

func (r *Message) SetDefault(i int) {
	switch i {
	}
	panic("Unknown field index")
}

func (r *Message) NullField(i int) {
	switch i {
	}
	panic("Not a nullable field index")
}

func (_ Message) AppendMap(key string) types.Field { panic("Unsupported operation") }
func (_ Message) AppendArray() types.Field         { panic("Unsupported operation") }
func (_ Message) HintSize(int)                     { panic("Unsupported operation") }
func (_ Message) Finalize()                        {}

func (_ Message) AvroCRC64Fingerprint() []byte {
	return []byte(MessageAvroCRC64Fingerprint)
}

func (r Message) MarshalJSON() ([]byte, error) {
	var err error
	output := make(map[string]json.RawMessage)
	output["payload"], err = json.Marshal(r.Payload)
	if err != nil {
		return nil, err
	}
	return json.Marshal(output)
}

func (r *Message) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}

	var val json.RawMessage
	val = func() json.RawMessage {
		if v, ok := fields["payload"]; ok {
			return v
		}
		return nil
	}()

	if val != nil {
		if err := json.Unmarshal([]byte(val), &r.Payload); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("no value specified for payload")
	}
	return nil
}
