package orm

import (
	"context"
	"fmt"
	"time"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func randomDBFile() string {
	return uuid.NewGenerator().NewString() + ".db"
}

// NewMockClient creates a mock orm client
func NewMockClient() (Client, error) {
	// ref:https://www.sqlite.org/inmemorydb.html
	// using dsn(file:%s?mode=memory&cache=shared) format here to
	// 1. Create different DB for different TestXXX()
	// 2. Enable DB shared for different connection
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", randomDBFile())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
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
