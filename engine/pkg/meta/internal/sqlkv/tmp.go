// NewSQLImpl return a sql implement of KVClient
func newSQLImpl(mc *metaclient.StoreConfigParams, sqlConf *sqlutil.DBConfig, table ) (*sqlImpl, error) {
	err := sqlutil.CreateDatabaseForProject(*mc, projectID, sqlConf)
	if err != nil {
		return nil, err
	}

	dsn := sqlutil.GenerateDSNByParams(*mc, projectID, sqlConf, true)
	sqlDB, err := sqlutil.NewSQLDB("mysql", dsn, sqlConf)
	if err != nil {
		return nil, err
	}

	cli, err := newImpl(sqlDB)
	if err != nil {
		sqlDB.Close()
	}

	return cli, err
}


// Initialize will create all related tables in SQL backend
// TODO: What if we change the definition of orm??
func (c *sqlImpl) Initialize(ctx context.Context) error {
	if err := c.db.WithContext(ctx).Table(c.tableName).AutoMigrate(&MetaKV{}); err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}
