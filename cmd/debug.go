package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc"
	"github.com/pingcap/ticdc/cdc/puller"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func init() {
	rootCmd.AddCommand(pullCmd)

	pullCmd.Flags().StringVar(&pdAddr, "pd-addr", "localhost:2379", "address of PD")
}

var pdAddr string

func getTableIDs() (tableIDs []int64, err error) {
	var (
		host     = "127.0.0.1"
		password = ""
		user     = "root"
		port     = 4000
		tableID  sql.NullInt64
		schemas  = make([]string, 0)
		schema   string
	)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&interpolateParams=true", user, password, host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return
	}

	sqlQuerySchemas := "SHOW DATABASES"
	result, err := db.Query(sqlQuerySchemas)
	if err != nil {
		return
	}
	for result.Next() {
		err = result.Scan(&schema)
		if err != nil {
			return
		}
		if cdc.IsSysSchema(schema) {
			continue
		}
		schemas = append(schemas, schema)
	}
	err = result.Close()
	if err != nil {
		return
	}

	ifconv := func(input []string) (ret []interface{}) {
		ret = make([]interface{}, len(input))
		for i, s := range input {
			ret[i] = s
		}
		return
	}
	sqlQuerySchema := "SELECT TIDB_TABLE_ID FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN (?" + strings.Repeat(",?", len(schemas)-1) + ")"
	result, err = db.Query(sqlQuerySchema, ifconv(schemas)...)
	if err != nil {
		return
	}
	for result.Next() {
		err = result.Scan(&tableID)
		if err != nil {
			return
		}
		tableIDs = append(tableIDs, tableID.Int64)
	}
	err = result.Close()
	return
}

var pullCmd = &cobra.Command{
	Use:   "pull",
	Short: "pull kv change and print out",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		cli, err := pd.NewClient(strings.Split(pdAddr, ","), pd.SecurityOption{})
		if err != nil {
			fmt.Println(err)
			return
		}

		g, ctx := errgroup.WithContext(context.Background())
		ts := oracle.ComposeTS(time.Now().Unix()*1000, 0)

		ddlPuller := puller.NewPuller(cli, ts, []util.Span{util.GetDDLSpan()}, false)
		ddlBuf := ddlPuller.Output()
		g.Go(func() error {
			return ddlPuller.Run(ctx)
		})
		g.Go(func() error {
			for {
				entry, err := ddlBuf.Get(ctx)
				if err != nil {
					return err
				}

				fmt.Printf("%+v\n", entry.GetValue())
			}
		})

		tableIDs, err := getTableIDs()
		if err != nil {
			fmt.Println(err)
			return
		}
		for _, tid := range tableIDs {
			dmlPuller := puller.NewPuller(cli, ts, []util.Span{util.GetTableSpan(tid, true)}, true)
			dmlBuf := dmlPuller.Output()
			g.Go(func() error {
				return dmlPuller.Run(ctx)
			})
			g.Go(func() error {
				for {
					entry, err := dmlBuf.Get(ctx)
					if err != nil {
						return err
					}

					fmt.Printf("%+v\n", entry.GetValue())
				}
			})
		}

		err = g.Wait()

		if err != nil {
			fmt.Println(err)
		}
	},
}
