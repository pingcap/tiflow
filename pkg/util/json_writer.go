// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"encoding/base64"
	"io"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

var jsonAPI = jsoniter.Config{
	EscapeHTML:  false,
	SortMapKeys: false,
}.Froze()

var jWriterPool = sync.Pool{
	New: func() interface{} {
		return &JSONWriter{}
	},
}

// JSONWriter builds JSON in an efficient and structural way.
//
// Example Usage
//
//	w := BorrowJSONWriter(out)
//
//	w.WriteObject(func() {
//	  w.WriteObjectField("payload", func() {
//	    w.WriteObjectField("dml", func() {
//	      w.WriteStringField("statement", "INSERT")
//	      w.WriteUint64Field("ts", 100)
//	    })
//	  })
//	  w.WriteObjectField("source", func() {
//	    w.WriteStringField("source", "TiCDC")
//	    w.WriteInt64Field("version", 1)
//	  })
//	})
//
//	ReturnJSONWriter(w)
type JSONWriter struct {
	out    io.Writer
	stream *jsoniter.Stream // `stream` is created over `out`

	needPrependComma bool
}

// BorrowJSONWriter borrows a JSONWriter instance from pool.
// Remember to call ReturnJSONWriter to return the borrowed instance.
func BorrowJSONWriter(out io.Writer) *JSONWriter {
	w := jWriterPool.Get().(*JSONWriter)
	w.out = out
	w.stream = jsonAPI.BorrowStream(out)
	w.needPrependComma = false
	return w
}

// ReturnJSONWriter returns the borrowed JSONWriter instance to pool.
func ReturnJSONWriter(w *JSONWriter) {
	w.stream.Flush()
	jsonAPI.ReturnStream(w.stream)
	w.out = nil
	w.stream = nil
	jWriterPool.Put(w)
}

// Buffer returns the buffer if out is nil.
// WARN: You may need to copy the result of the buffer. Otherwise the content of the buffer
// may be changed.
func (w *JSONWriter) Buffer() []byte {
	return w.stream.Buffer()
}

// WriteBase64String writes a base64 string like "<value>".
func (w *JSONWriter) WriteBase64String(b []byte) {
	if w.out == nil {
		w.stream.WriteRaw(`"`)
		encoder := base64.NewEncoder(base64.StdEncoding, w.stream)
		_, _ = encoder.Write(b)
		_ = encoder.Close()
		w.stream.WriteRaw(`"`)
	} else {
		// If out is available, let's write to out directly to avoid extra copy.
		// As we write to out directly so we need to flush the jsoniter stream first.
		_ = w.stream.Flush()
		_, _ = w.out.Write([]byte(`"`))
		encoder := base64.NewEncoder(base64.StdEncoding, w.out)
		_, _ = encoder.Write(b)
		_ = encoder.Close()
		_, _ = w.out.Write([]byte(`"`))
	}
}

// WriteObject writes {......}.
func (w *JSONWriter) WriteObject(objectFieldsWriteFn func()) {
	lastNeedPrependComma := w.needPrependComma
	w.needPrependComma = false
	w.stream.WriteObjectStart()
	objectFieldsWriteFn()
	w.stream.WriteObjectEnd()
	w.needPrependComma = lastNeedPrependComma
}

// WriteBoolField writes a bool field like "<fieldName>":<value>.
func (w *JSONWriter) WriteBoolField(fieldName string, value bool) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.stream.WriteBool(value)
}

// WriteIntField writes a int field like "<fieldName>":<value>.
func (w *JSONWriter) WriteIntField(fieldName string, value int) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.stream.WriteInt(value)
}

// WriteInt64Field writes a int64 field like "<fieldName>":<value>.
func (w *JSONWriter) WriteInt64Field(fieldName string, value int64) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.stream.WriteInt64(value)
}

// WriteUint64Field writes a uint64 field like "<fieldName>":<value>.
func (w *JSONWriter) WriteUint64Field(fieldName string, value uint64) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.stream.WriteUint64(value)
}

// WriteFloat64Field writes a float64 field like "<fieldName>":<value>.
func (w *JSONWriter) WriteFloat64Field(fieldName string, value float64) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.stream.WriteFloat64(value)
}

// WriteStringField writes a string field like "<fieldName>":"<value>".
func (w *JSONWriter) WriteStringField(fieldName string, value string) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.stream.WriteString(value)
}

// WriteBase64StringField writes a base64 string field like "<fieldName>":"<value>".
func (w *JSONWriter) WriteBase64StringField(fieldName string, b []byte) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.WriteBase64String(b)
}

// WriteAnyField writes a field like "<fieldName>":<value>.
func (w *JSONWriter) WriteAnyField(fieldName string, value any) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.stream.WriteVal(value)
}

// WriteObjectField writes a object field like "<fieldName>":{......}.
func (w *JSONWriter) WriteObjectField(fieldName string, objectFieldsWriteFn func()) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.WriteObject(objectFieldsWriteFn)
}

// WriteNullField writes a array field like "<fieldName>":null.
func (w *JSONWriter) WriteNullField(fieldName string) {
	if w.needPrependComma {
		w.stream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.stream.WriteObjectField(fieldName)
	w.stream.WriteNil()
}
