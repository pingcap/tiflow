package cdc

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pingcap/parser/model"
)

// Sink is an abstraction for anything that a changefeed may emit into.
type Sink interface {
	EmitRow(
		ctx context.Context,
		table *model.TableInfo,
		key, value []byte,
		ts uint64,
	) error
	EmitResolvedTimestamp(
		ctx context.Context,
		encoder Encoder,
		resolved uint64,
	) error
	// Flush blocks until every message enqueued by EmitRow and
	// EmitResolvedTimestamp has been acknowledged by the sink.
	Flush(ctx context.Context) error
	// Close does not guarantee delivery of outstanding messages.
	Close() error
}

func getSink(
	sinkURI string,
	opts map[string]string,
) (Sink, error) {
	// TODO
	return &writerSink{Writer: os.Stdout}, nil
}

type writerSink struct {
	io.Writer
}

var _ Sink = &writerSink{}

func (s *writerSink) EmitRow(ctx context.Context, table *model.TableInfo, key, value []byte, updated uint64) error {
	fmt.Fprintf(s, "key: %s, value: %s, updated: %d", string(key), string(value), updated)
	return nil
}

func (s *writerSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved uint64) error {
	fmt.Fprintf(s, "resolved: %d", resolved)
	return nil
}

func (s *writerSink) Flush(ctx context.Context) error {
	return nil
}

func (s *writerSink) Close() error {
	return nil
}
