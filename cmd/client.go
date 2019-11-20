package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(cliCmd)

	cliCmd.Flags().StringSliceVar(&tables, "tables", []string{"test.t"}, "all tables provided will be collected in changefeed")
	cliCmd.Flags().StringSliceVar(&databases, "databases", []string{"test"}, "all tables in given databases will be collected in changefeed")
	cliCmd.Flags().StringVar(&pdAddress, "pd-addr", "localhost:2379", "address of PD")
	cliCmd.Flags().Uint64Var(&startTs, "start-ts", 0, "start ts of changefeed")
}

var (
	tables    []string
	databases []string
	pdAddress string
	startTs   uint64
)

var cliCmd = &cobra.Command{
	Use:   "cli",
	Short: "simulate client to create changefeed",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
		var (
			host     = "127.0.0.1"
			password = ""
			user     = "root"
			port     = 4000
			tableIDs = make(map[int64]struct{})
			tableID  sql.NullInt64
		)
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true", user, password, host, port)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return err
		}

		if len(databases) > 0 {
			ifconv := func(input []string) (ret []interface{}) {
				ret = make([]interface{}, len(input))
				for i, s := range input {
					ret[i] = s
				}
				return
			}
			sqlQuerySchema := "SELECT TIDB_TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN (?" + strings.Repeat(",?", len(databases)-1) + ")"
			result, err := db.Query(sqlQuerySchema, ifconv(databases)...)
			if err != nil {
				return err
			}
			for result.Next() {
				err = result.Scan(&tableID)
				if err != nil {
					return err
				}
				tableIDs[tableID.Int64] = struct{}{}
			}
			result.Close()
		}

		sqlQueryTable := "SELECT TIDB_TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
		for _, tbl := range tables {
			subs := strings.Split(tbl, ".")
			if len(subs) != 2 {
				return errors.Errorf("%s is not schema.table format", tbl)
			}
			result, err := db.Query(sqlQueryTable, subs[0], subs[1])
			if err != nil {
				return err
			}
			for result.Next() {
				err = result.Scan(&tableID)
				if err != nil {
					return err
				}
				tableIDs[tableID.Int64] = struct{}{}
				break
			}
			result.Close()
		}

		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{pdAddress},
			DialTimeout: 5 * time.Second,
			DialOptions: []grpc.DialOption{
				grpc.WithBackoffMaxDelay(time.Second * 3),
			},
		})
		if err != nil {
			return err
		}
		ids := make([]uint64, 0, len(tableIDs))
		for k := range tableIDs {
			ids = append(ids, uint64(k))
		}
		id := uuid.New().String()
		detail := &model.ChangeFeedDetail{
			SinkURI:    "root@tcp(127.0.0.1:3306)/test",
			Opts:       make(map[string]string),
			CreateTime: time.Now(),
			TableIDs:   ids,
			StartTs:    startTs,
		}
		fmt.Printf("create changefeed detail %+v\n", detail)
		return kv.SaveChangeFeedDetail(context.Background(), cli, detail, id)
	},
}
