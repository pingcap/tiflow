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

package large_message_handle

import (
	"bytes"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4/v4"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/prometheus/client_golang/prometheus"
)

type Codec string

const (
	// None no compression
	None Codec = "none"
	// Snappy compression
	Snappy Codec = "snappy"
	// LZ4 compression
	LZ4 Codec = "lz4"
)

func Supported(codec string) bool {
	switch Codec(codec) {
	case None, Snappy, LZ4:
		return true
	}
	return false
}

type Compressor struct {
	cc Codec

	compressRatio prometheus.Observer
}

func NewCompressor(changefeedID model.ChangeFeedID, codec string) *Compressor {
	return &Compressor{
		compressRatio: compressionRatio.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
		cc:            Codec(codec),
	}
}

func CleanMetrics(changefeedID model.ChangeFeedID) {
	compressionRatio.DeleteLabelValues(changefeedID.Namespace, changefeedID.ID)
}

func (c *Compressor) Encode(data []byte) ([]byte, error) {
	oldSize := len(data)
	var compressed []byte

	switch c.cc {
	case None:
		return data, nil
	case Snappy:
		compressed = snappy.Encode(data)
	case LZ4:
		var buf bytes.Buffer
		writer := lz4.NewWriter(&buf)
		if _, err := writer.Write(data); err != nil {
			return nil, errors.Trace(err)
		}
		if err := writer.Close(); err != nil {
			return nil, errors.Trace(err)
		}
		compressed = buf.Bytes()
	default:
		return nil, errors.New("unsupported compression codec")
	}

	ratio := float64(oldSize) / float64(len(compressed)) * 100
	c.compressRatio.Observe(ratio)

	return compressed, nil
}

func (c *Compressor) Decode(data []byte) ([]byte, error) {
	switch c.cc {
	case None:
		return data, nil
	case Snappy:
		return snappy.Decode(data)
	case LZ4:
		reader := lz4.NewReader(bytes.NewReader(data))
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(reader); err != nil {
			return nil, errors.Trace(err)
		}
		return buf.Bytes(), nil
	default:
	}
	return nil, errors.New("unsupported compression codec")
}
