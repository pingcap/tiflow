package orm

import (
	"context"
	"time"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// NewMockClient creates a mock orm client
func NewMockClient() (Client, error) {
	// ref:https://www.sqlite.org/inmemorydb.html
	// TODO: Opening in-memory db with shared cache can avoid new DB create
	// when starting a new connection. But it will cause other cases fail because
	// we has enabled 't.Parallel()'. Cases will share the same in-memory DB if they are
	// in same process.
	// db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	if err != nil {
		log.L().Error("create gorm client fail", zap.Error(err))
		return nil, cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	cli := &metaOpsClient{
		db: db,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cli.Initialize(ctx); err != nil {
		cli.Close()
		return nil, err
	}

	return cli, nil
}
