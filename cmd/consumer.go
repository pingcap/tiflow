package cmd

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/spf13/cobra"
	"log"
)

func init() {
	rootCmd.AddCommand(consumerCmd)

	consumerCmd.Flags().StringVar(&kafkaVersion, "kafka-version", "2.11-0.10.0.0", "kafka version")
	consumerCmd.Flags().StringVar(&kafkaTopic, "kafka-topic", "cdc", "kafka topic")
	consumerCmd.Flags().StringVar(&kafkaAddr, "kafka-addr", "127.0.0.1:9092", "address of kafka")
	consumerCmd.Flags().StringVar(&consumerSinkURI, "sink-uri", "root@tcp(127.0.0.1:3306)/test", "sink uri")
}

var (
	consumerSinkURI string
	kafkaVersion    string
	kafkaAddr       string
	kafkaTopic      string
)

var consumerCmd = &cobra.Command{
	Use:   "consumer",
	Short: "kafka consumer to process open cdc protocol",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {

		consumer, err := sink.NewMessageConsumer(&DummySink{}, kafkaVersion, kafkaAddr, kafkaTopic)
		if err != nil {
			log.Fatal(err)
		}
		consumer.Start(context.Background())
		return nil
	},
}

type DummySink struct {
}

func (s *DummySink) Emit(ctx context.Context, t model.Txn) error {
	fmt.Printf("commit ts: %d, %v, %v", t.Ts, t.DMLs, t.DDL)
	return nil
}

func (s *DummySink) EmitResolvedTimestamp(ctx context.Context, resolved uint64) error {
	fmt.Printf("resolved: %d", resolved)
	return nil
}

func (s *DummySink) Flush(ctx context.Context) error {
	return nil
}

func (s *DummySink) Close() error {
	return nil
}
