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

	"github.com/stretchr/testify/suite"
	"github.com/thanhpk/randstr"
)

type JSONWriterTestSuite struct {
	suite.Suite
	useInternalBuffer bool
}

func (suite *JSONWriterTestSuite) writeJSON(fn func(*JSONWriter)) string {
	if suite.useInternalBuffer {
		w := BorrowJSONWriter(nil)
		fn(w)
		ret := string(w.Buffer())
		ReturnJSONWriter(w)
		return ret
	}

	out := &bytes.Buffer{}
	w := BorrowJSONWriter(out)
	fn(w)
	ReturnJSONWriter(w)
	return out.String()
}

func (suite *JSONWriterTestSuite) TestObject() {
	var s string

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {})
	})
	suite.Require().Equal(`{}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteAnyField("foo", 1)
		})
	})
	suite.Require().Equal(`{"foo":1}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteAnyField("foo", 1)
			w.WriteAnyField("bar", 2)
		})
	})
	suite.Require().Equal(`{"foo":1,"bar":2}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {})
		})
	})
	suite.Require().Equal(`{"foo":{}}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {})
			w.WriteObjectField("bar", func() {})
		})
	})
	suite.Require().Equal(`{"foo":{},"bar":{}}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteObjectField("foo1", func() {})
			})
			w.WriteObjectField("bar", func() {
				w.WriteObjectField("foo2", func() {})
			})
		})
	})
	suite.Require().Equal(`{"foo":{"foo1":{}},"bar":{"foo2":{}}}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
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
	suite.Require().Equal(`{"foo":{"foo1":{},"bar1":{}},"bar":{"foo2":{}}}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
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
	suite.Require().Equal(`{"foo":{"foo1":{},"bar1":{}},"bar":{"foo2":{},"bar2":{}}}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
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
	suite.Require().Equal(`{"foo":{"foo1":{"abc":null},"bar1":{}},"bar":{"foo2":{},"bar2":{}}}`, s)
}

func (suite *JSONWriterTestSuite) TestBase64() {
	var s string

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteBase64String([]byte("foo"))
	})
	suite.Require().Equal(`"Zm9v"`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteBase64StringField("foo", []byte("bar"))
		})
	})
	suite.Require().Equal(`{"foo":"YmFy"}`, s)
}

func (suite *JSONWriterTestSuite) TestField() {
	var s string

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteStringField("foo", "bar")
		})
	})
	suite.Require().Equal(`{"foo":"bar"}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteUint64Field("foo", 1)
		})
	})
	suite.Require().Equal(`{"foo":1}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteFloat64Field("foo", 1.1)
		})
	})
	suite.Require().Equal(`{"foo":1.1}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteNullField("foo")
		})
	})
	suite.Require().Equal(`{"foo":null}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteAnyField("foo", nil)
		})
	})
	suite.Require().Equal(`{"foo":null}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteAnyField("foo", 1)
		})
	})
	suite.Require().Equal(`{"foo":1}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteBoolField("foo", true)
		})
	})
	suite.Require().Equal(`{"foo":true}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteBoolField("foo", false)
		})
	})
	suite.Require().Equal(`{"foo":false}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {})
		})
	})
	suite.Require().Equal(`{"foo":{}}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteUint64Field("bar", 1)
			})
		})
	})
	suite.Require().Equal(`{"foo":{"bar":1}}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteObjectField("foo", func() {
				w.WriteUint64Field("bar", 1)
				w.WriteStringField("abc", "def")
			})
		})
	})
	suite.Require().Equal(`{"foo":{"bar":1,"abc":"def"}}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteUint64Field("bar", 1)
			w.WriteStringField("abc", "def")
			w.WriteNullField("xyz")
		})
	})
	suite.Require().Equal(`{"bar":1,"abc":"def","xyz":null}`, s)

	s = suite.writeJSON(func(w *JSONWriter) {
		w.WriteObject(func() {
			w.WriteUint64Field("bar", 1)
			w.WriteStringField("abc", "def")
			w.WriteNullField("xyz")
			w.WriteObjectField("foo", func() {})
		})
	})
	suite.Require().Equal(`{"bar":1,"abc":"def","xyz":null,"foo":{}}`, s)
}

func TestExternalBuffer(t *testing.T) {
	suite.Run(t, &JSONWriterTestSuite{useInternalBuffer: false})
}

func TestInternalBuffer(t *testing.T) {
	suite.Run(t, &JSONWriterTestSuite{useInternalBuffer: true})
}

func BenchmarkExternalBufferWriteBase64(b *testing.B) {
	out := &bytes.Buffer{}
	str := randstr.Bytes(1024)
	for i := 0; i < b.N; i++ {
		out.Reset()
		w := BorrowJSONWriter(out)
		w.WriteObject(func() {
			w.WriteBase64StringField("foo", str)
		})
		_ = out.Bytes()
		ReturnJSONWriter(w)
	}
}

func BenchmarkInternalBufferWriteBase64(b *testing.B) {
	str := randstr.Bytes(1024)
	for i := 0; i < b.N; i++ {
		w := BorrowJSONWriter(nil)
		w.WriteObject(func() {
			w.WriteBase64StringField("foo", str)
		})
		_ = w.Buffer()
		ReturnJSONWriter(w)
	}
}

func BenchmarkExternalBufferWriteString(b *testing.B) {
	out := &bytes.Buffer{}
	str := randstr.String(1024)
	for i := 0; i < b.N; i++ {
		out.Reset()
		w := BorrowJSONWriter(out)
		w.WriteObject(func() {
			w.WriteStringField("foo", str)
		})
		_ = out.Bytes()
		ReturnJSONWriter(w)
	}
}

func BenchmarkInternalBufferWriteString(b *testing.B) {
	str := randstr.String(1024)
	for i := 0; i < b.N; i++ {
		w := BorrowJSONWriter(nil)
		w.WriteObject(func() {
			w.WriteStringField("foo", str)
		})
		_ = w.Buffer()
		ReturnJSONWriter(w)
	}
}
