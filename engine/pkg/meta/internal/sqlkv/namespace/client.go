package namespace

import (
	"time"

	"github.com/pingcap/tiflow/engine/pkg/meta/internal/sqlkv"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
)

func NewClient(mc *metaclient.StoreConfigParams, projectID ProjectID, jobID JobID) (metaclient.KVClient, error) {
	ormDB, err := sqlkv.NewOrmDB(mc)
	if err != nil {
		return nil, err
	}

	// specified self-define table name
	nsOrmDB := ormDB.Table(makeTableName(projectID))
	ctx, cancel := context.WithTimeout(context.Backgroud(), 3*time.Second)
	defer cancel()

	if err := nsOrmDB.WithContext(ctx).AutoMigrate(&sqlkv.MetaKV{}); err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return sqlkv.NewSQLImpl(nsOrmDB.Where("job_id = ?", jobID))
}

func makeTableName(projectID ProjectID) string {
	return projectID
}
