// Copyright 2023 PingCAP, Inc.
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

package compression

import (
	"bytes"

	"github.com/klauspost/compress/snappy"
	"github.com/pierrec/lz4/v4"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

const (
	// None no compression
	None string = "none"

	// Snappy compression
	Snappy string = "snappy"

	// LZ4 compression
	LZ4 string = "lz4"
)

// Supported return true if the given compression is supported.
func Supported(cc string) bool {
	switch cc {
	case None, Snappy, LZ4:
		return true
	}
	return false
}

// Encode the given data by the given compression codec.
func Encode(cc string, data []byte) ([]byte, error) {
	switch cc {
	case None:
		return data, nil
	case Snappy:
		return snappy.Encode(nil, data), nil
	case LZ4:
		var buf bytes.Buffer
		writer := lz4.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return nil, cerror.WrapError(cerror.ErrCompressionFailed, err)
		}
		if err := writer.Close(); err != nil {
			return nil, cerror.WrapError(cerror.ErrCompressionFailed, err)
		}
		return buf.Bytes(), nil
	default:
	}

	return nil, cerror.ErrCompressionFailed.GenWithStack("Unsupported compression %s", cc)
}

// Decode the given data by the given compression codec.
func Decode(cc string, data []byte) ([]byte, error) {
	switch cc {
	case None:
		return data, nil
	case Snappy:
		return snappy.Decode(nil, data)
	case LZ4:
		reader := lz4.NewReader(bytes.NewReader(data))
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(reader); err != nil {
			return nil, cerror.WrapError(cerror.ErrCompressionFailed, err)
		}
		return buf.Bytes(), nil
	default:
	}

	return nil, cerror.ErrCompressionFailed.GenWithStack("Unsupported compression %s", cc)
}
