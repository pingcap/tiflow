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

package common

import (
	"bytes"
	"strings"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type CompressionCodec uint8

const (
	// CompressionNone no compression
	CompressionNone CompressionCodec = iota
	// CompressionSnappy compression using snappy
	CompressionSnappy
	// CompressionLZ4 compression using LZ4
	CompressionLZ4
)

var compressionNames = []string{"none", "snappy", "lz4"}

func GetCompressionCodec(name string) CompressionCodec {
	name = strings.ToLower(strings.TrimSpace(name))
	for i, n := range compressionNames {
		if n == name {
			return CompressionCodec(i)
		}
	}

	log.Warn("cannot found the compression codec", zap.String("name", name))
	return CompressionNone
}

func (c CompressionCodec) String() string {
	return compressionNames[c]
}

func Compress(cc CompressionCodec, data []byte) ([]byte, error) {
	switch cc {
	case CompressionNone:
		return data, nil
	case CompressionSnappy:
		return snappy.Encode(data), nil
	case CompressionLZ4:
		var buf bytes.Buffer
		writer := lz4.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return nil, errors.Trace(err)
		}
		if err := writer.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		return buf.Bytes(), nil
	default:
	}
	return nil, errors.New("unsupported compression codec")
}
