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

type JSONWriter struct {
	out              io.Writer
	needPrependComma bool
	rawStream        *jsoniter.Stream
}

func BorrowJSONWriter(out io.Writer) *JSONWriter {
	w := jWriterPool.Get().(*JSONWriter)
	w.rawStream = jsonAPI.BorrowStream(out)
	w.needPrependComma = false
	w.out = out
	return w
}

func ReturnJSONWriter(w *JSONWriter) {
	w.rawStream.Flush()
	w.rawStream = nil
	w.out = nil
	jWriterPool.Put(w)
}

func (w *JSONWriter) WriteBase64String(b []byte) {
	// As we write to out directly so we need to flush the jsoniter stream first.
	w.rawStream.Flush()
	w.out.Write([]byte(`"`))
	encoder := base64.NewEncoder(base64.StdEncoding, w.out)
	_, _ = encoder.Write(b)
	encoder.Close()
	w.out.Write([]byte(`"`))
}

func (w *JSONWriter) WriteObject(objectFieldsWriteFn func()) {
	lastNeedPrependComma := w.needPrependComma
	w.needPrependComma = false
	w.rawStream.WriteObjectStart()
	objectFieldsWriteFn()
	w.rawStream.WriteObjectEnd()
	w.needPrependComma = lastNeedPrependComma
}

func (w *JSONWriter) WriteBoolField(fieldName string, value bool) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.rawStream.WriteBool(value)
}

func (w *JSONWriter) WriteIntField(fieldName string, value int) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.rawStream.WriteInt(value)
}

func (w *JSONWriter) WriteInt64Field(fieldName string, value int64) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.rawStream.WriteInt64(value)
}

func (w *JSONWriter) WriteUint64Field(fieldName string, value uint64) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.rawStream.WriteUint64(value)
}

func (w *JSONWriter) WriteFloat64Field(fieldName string, value float64) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.rawStream.WriteFloat64(value)
}

func (w *JSONWriter) WriteStringField(fieldName string, value string) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.rawStream.WriteString(value)
}

func (w *JSONWriter) WriteBase64StringField(fieldName string, b []byte) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.WriteBase64String(b)
}

func (w *JSONWriter) WriteAnyField(fieldName string, value any) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.rawStream.WriteVal(value)
}

func (w *JSONWriter) WriteObjectField(fieldName string, objectFieldsWriteFn func()) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.WriteObject(objectFieldsWriteFn)
}

func (w *JSONWriter) WriteNullField(fieldName string) {
	if w.needPrependComma {
		w.rawStream.WriteMore()
	} else {
		w.needPrependComma = true
	}
	w.rawStream.WriteObjectField(fieldName)
	w.rawStream.WriteNil()
}
