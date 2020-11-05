package sink

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
)

func init() {
	failpoint.Inject("SimpleMySQLSinkTester", func() {
		sinkIniterMap["simple-mysql"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
			filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
			return newSimpleMySQLSink(ctx, sinkURI, config, opts)
		}
	})
}

type simpleMySQLSink struct {
	enableOldValue bool
	checkOldValue  bool
	db             *sql.DB
}

func newSimpleMySQLSink(ctx context.Context, sinkURI *url.URL, config *config.ReplicaConfig, opts map[string]string) (*simpleMySQLSink, error) {
	var db *sql.DB

	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	username := sinkURI.User.Username()
	password, _ := sinkURI.User.Password()
	port := sinkURI.Port()
	if username == "" {
		username = "root"
	}
	if port == "" {
		port = "4000"
	}

	dsnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/", username, password, sinkURI.Hostname(), port)
	dsn, err := dmysql.ParseDSN(dsnStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}

	// create test db used for parameter detection
	if dsn.Params == nil {
		dsn.Params = make(map[string]string, 1)
	}
	testDB, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrMySQLConnectionError, err), "fail to open MySQL connection when configuring sink")
	}
	defer testDB.Close()

	db, err = sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrMySQLConnectionError, err), "Open database connection failed")
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrMySQLConnectionError, err), "fail to open MySQL connection")
	}

	sink := &simpleMySQLSink{
		db:             db,
		enableOldValue: config.EnableOldValue,
	}
	if checkOldValue, ok := opts["check-old-value"]; ok {
		sink.checkOldValue = strings.ToLower(checkOldValue) == "true"
	}
	return sink, nil
}

func (s *simpleMySQLSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// do nothing
	return nil
}

// EmitRowChangedEvents sends Row Changed Event to Sink
// EmitRowChangedEvents may write rows to downstream directly;
func (s *simpleMySQLSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	var sql string
	var args []interface{}
	if s.enableOldValue {
		for _, row := range rows {
			if len(row.PreColumns) != 0 && len(row.Columns) != 0 {
				// update
				sql, args = prepareUpdate(row.Table.QuoteString(), row.PreColumns, row.Columns, true)
			} else if len(row.PreColumns) == 0 {
				// insert
				sql, args = prepareReplace(row.Table.QuoteString(), row.Columns, true, true)
			} else if len(row.Columns) == 0 {
				// delete
				sql, args = prepareDelete(row.Table.QuoteString(), row.PreColumns, true)
			}
			_, err := s.db.ExecContext(ctx, sql, args...)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		for _, row := range rows {
			if row.IsDelete() {
				sql, args = prepareDelete(row.Table.QuoteString(), row.PreColumns, true)
			} else {
				sql, args = prepareReplace(row.Table.QuoteString(), row.Columns, true, false)
			}
			_, err := s.db.ExecContext(ctx, sql, args...)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// EmitDDLEvent sends DDL Event to Sink
// EmitDDLEvent should execute DDL to downstream synchronously
func (s *simpleMySQLSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	sql := fmt.Sprintf("use %s;%s", ddl.TableInfo.Schema, ddl.Query)
	_, err := s.db.ExecContext(ctx, sql)
	return err
}

// FlushRowChangedEvents flushes each row which of commitTs less than or equal to `resolvedTs` into downstream.
// TiCDC guarantees that all of Event which of commitTs less than or equal to `resolvedTs` are sent to Sink through `EmitRowChangedEvents`
func (s *simpleMySQLSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	return resolvedTs, nil
}

// EmitCheckpointTs sends CheckpointTs to Sink
// TiCDC guarantees that all Events **in the cluster** which of commitTs less than or equal `checkpointTs` are sent to downstream successfully.
func (s *simpleMySQLSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// do nothing
	return nil
}

// Close closes the Sink
func (s *simpleMySQLSink) Close() error {
	return s.db.Close()
}
