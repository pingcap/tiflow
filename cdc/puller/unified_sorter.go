package puller

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"io"
	"os"
	"syscall"
)

import "github.com/grandecola/mmap"

type persister interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
}

type filePersister struct {
	file *mmap.File
	name string
}

func newFilePersister(fileName string, size int64) (*filePersister, error) {
	fd, err := os.Create(fileName)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	err = fd.Truncate(size)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	mappedFile, err := mmap.NewSharedFileMmap(fd, 0, int(size), syscall.PROT_READ | syscall.PROT_WRITE)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	err = mappedFile.Advise(syscall.MADV_SEQUENTIAL)
	if err != nil {
		return nil, errors.AddStack(err)
	}

	log.Debug("mmapped file", zap.String("file-name", fileName), zap.Int64("size", size))

	return &filePersister{
		file: mappedFile,
		name: fileName,
	}, nil
}

func (f *filePersister) ReadAt(p []byte, offset int64) (n int, err error) {
	return f.file.ReadAt(p, offset)
}

func (f *filePersister) WriteAt(p []byte, offset int64) (n int, err error) {
	return f.file.WriteAt(p, offset)
}

func (f *filePersister) Close() error {
	err := f.file.Unmap()
	if err != nil {
		return err
	}
	log.Debug("closed file", zap.String("file-name", f.name))
	return err
}

func (f *filePersister) Flush() error {
	err := f.file.Flush(syscall.MS_ASYNC)
	if err != nil {
		return err
	}
	log.Debug("flushed file", zap.String("file-name", f.name))
	return err
}



