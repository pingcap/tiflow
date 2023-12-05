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
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeJSON(fn func(*JSONWriter)) string {
	out := &bytes.Buffer{}
	w := BorrowJSONWriter(out)
	fn(w)
	ReturnJSONWriter(w)
	return out.String()
}

func TestObject(t *testing.T) {
	var s string

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {})
	})
	require.Equal(t, `{}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteAnyField("foo", 1)
		})
	})
	require.Equal(t, `{"foo":1}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteAnyField("foo", 1)
			w.WriteAnyField("bar", 2)
		})
	})
	require.Equal(t, `{"foo":1,"bar":2}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {})
		})
	})
	require.Equal(t, `{"foo":{}}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {})
			w.WriteObjectField("bar", func() {})
		})
	})
	require.Equal(t, `{"foo":{},"bar":{}}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteObjectField("foo1", func() {})
			})
			w.WriteObjectField("bar", func() {
				w.WriteObjectField("foo2", func() {})
			})
		})
	})
	require.Equal(t, `{"foo":{"foo1":{}},"bar":{"foo2":{}}}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteObjectField("foo1", func() {})
				w.WriteObjectField("bar1", func() {})
			})
			w.WriteObjectField("bar", func() {
				w.WriteObjectField("foo2", func() {})
			})
		})
	})
	require.Equal(t, `{"foo":{"foo1":{},"bar1":{}},"bar":{"foo2":{}}}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteObjectField("foo1", func() {})
				w.WriteObjectField("bar1", func() {})
			})
			w.WriteObjectField("bar", func() {
				w.WriteObjectField("foo2", func() {})
				w.WriteObjectField("bar2", func() {})
			})
		})
	})
	require.Equal(t, `{"foo":{"foo1":{},"bar1":{}},"bar":{"foo2":{},"bar2":{}}}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteObjectField("foo1", func() {
					w.WriteNullField("abc")
				})
				w.WriteObjectField("bar1", func() {})
			})
			w.WriteObjectField("bar", func() {
				w.WriteObjectField("foo2", func() {})
				w.WriteObjectField("bar2", func() {})
			})
		})
	})
	require.Equal(t, `{"foo":{"foo1":{"abc":null},"bar1":{}},"bar":{"foo2":{},"bar2":{}}}`, s)
}

func TestBase64(t *testing.T) {
	var s string

	s = writeJSON(func(w *JSONWriter) {
		w.WriteBase64String([]byte("foo"))
	})
	require.Equal(t, `"Zm9v"`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteBase64StringField("foo", []byte("bar"))
		})
	})
	require.Equal(t, `{"foo":"YmFy"}`, s)
}

func TestField(t *testing.T) {
	var s string

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteStringField("foo", "bar")
		})
	})
	require.Equal(t, `{"foo":"bar"}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteUint64Field("foo", 1)
		})
	})
	require.Equal(t, `{"foo":1}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteFloat64Field("foo", 1.1)
		})
	})
	require.Equal(t, `{"foo":1.1}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteNullField("foo")
		})
	})
	require.Equal(t, `{"foo":null}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteAnyField("foo", nil)
		})
	})
	require.Equal(t, `{"foo":null}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteAnyField("foo", 1)
		})
	})
	require.Equal(t, `{"foo":1}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteBoolField("foo", true)
		})
	})
	require.Equal(t, `{"foo":true}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteBoolField("foo", false)
		})
	})
	require.Equal(t, `{"foo":false}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {})
		})
	})
	require.Equal(t, `{"foo":{}}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteUint64Field("bar", 1)
			})
		})
	})
	require.Equal(t, `{"foo":{"bar":1}}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteUint64Field("bar", 1)
				w.WriteStringField("abc", "def")
			})
		})
	})
	require.Equal(t, `{"foo":{"bar":1,"abc":"def"}}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteUint64Field("bar", 1)
			w.WriteStringField("abc", "def")
			w.WriteNullField("xyz")
		})
	})
	require.Equal(t, `{"bar":1,"abc":"def","xyz":null}`, s)

	s = writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteUint64Field("bar", 1)
			w.WriteStringField("abc", "def")
			w.WriteNullField("xyz")
			w.WriteObjectField("foo", func() {})
		})
	})
	require.Equal(t, `{"bar":1,"abc":"def","xyz":null,"foo":{}}`, s)
}
