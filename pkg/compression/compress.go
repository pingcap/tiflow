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
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/pingcap/errors"
)

const (
	// None no compression
	None string = "none"

	// GZIP compression
	GZIP string = "gzip"

	// Snappy compression
	Snappy string = "snappy"

	// LZ4 compression
	LZ4 string = "lz4"

	// ZSTD compression
	ZSTD string = "zstd"
)

func Supported(cc string) bool {
	switch cc {
	case None, Snappy, LZ4:
		return true
	}
	return false
}

func Encode(cc string, data []byte) ([]byte, error) {
	switch cc {
	case None:
		return data, nil
	case GZIP:
		var buf bytes.Buffer
		writer := gzip.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case Snappy:
		return snappy.Encode(nil, data), nil
	case LZ4:
		var buf bytes.Buffer
		writer := lz4.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return nil, errors.Trace(err)
		}
		if err := writer.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		return buf.Bytes(), nil
	case ZSTD:
		var buf bytes.Buffer
		zstdEncoder, err := zstd.NewWriter(&buf, zstd.WithZeroFrames(true),
			zstd.WithEncoderLevel(zstd.SpeedDefault),
			zstd.WithEncoderConcurrency(1))
		if err != nil {
			return nil, errors.Trace(err)
		}
		if _, err := zstdEncoder.Write(data); err != nil {
			return nil, errors.Trace(err)
		}
		if err := zstdEncoder.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		return buf.Bytes(), nil
	default:
	}

	return nil, errors.New("unsupported compression codec")
}

func Decode(cc string, data []byte) ([]byte, error) {
	switch cc {
	case None:
		return data, nil
	case GZIP:
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, errors.Trace(err)
		}
		result, err := io.ReadAll(reader)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return result, nil
	case Snappy:
		return snappy.Decode(nil, data)
	case LZ4:
		reader := lz4.NewReader(bytes.NewReader(data))
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(reader); err != nil {
			return nil, errors.Trace(err)
		}
		return buf.Bytes(), nil
	case ZSTD:
		reader, err := zstd.NewReader(bytes.NewReader(data), zstd.WithDecoderConcurrency(0))
		if err != nil {
			return nil, errors.Trace(err)
		}
		result, err := reader.DecodeAll(data, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return result, nil
	default:
	}
	return nil, errors.New("unsupported compression codec")
}
