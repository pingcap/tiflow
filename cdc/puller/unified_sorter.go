package puller

import (
	"bufio"
	"encoding"
	"go.uber.org/zap"
	"os"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
)

const fileBufferSize = 16 * 1024 * 1024

type sorterBackEnd interface {
	readNext(*model.PolymorphicEvent) error
	writeNext(event *model.PolymorphicEvent) error
}

type fileSorterBackEnd struct {
	f *os.File
	buf *bufio.ReadWriter
	serde serializerDeserializer
	name string
}

type serializerDeserializer interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

func newFileSorterBackEnd(fileName string, serde serializerDeserializer) (*fileSorterBackEnd, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	reader := bufio.NewReaderSize(f, fileBufferSize)
	writer := bufio.NewWriterSize(f, fileBufferSize)
	buf := bufio.NewReadWriter(reader, writer)

	log.Debug("new FileSorterBackEnd created", zap.String("filename", fileName))
	return &fileSorterBackEnd{
		f: f,
		buf: buf,
		serde: serde,
		name: fileName}, nil
}

func (f *fileSorterBackEnd) readNext(*model.PolymorphicEvent) error {
	
}

func (f *fileSorterBackEnd) writeNext(event *model.PolymorphicEvent) error {
	panic("implement me")
}


