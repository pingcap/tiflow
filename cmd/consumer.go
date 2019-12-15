package cmd

import (
	"context"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/ticdc/cdc/sink"
	"github.com/spf13/cobra"
	"log"
)

func init() {
	rootCmd.AddCommand(consumerCmd)

	consumerCmd.Flags().StringVar(&kafkaVersion, "kafka-version", "2.11.10", "kafka version")
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

		consumer, infoGetter, err := sink.NewMessageConsumer(kafkaVersion, kafkaAddr, kafkaTopic)
		if err != nil {
			log.Fatal("failed to init kafka consumer", err)
		}
		sink, err := sink.NewMySQLSink(consumerSinkURI, infoGetter, map[string]string{})
		if err != nil {
			log.Fatal("start mysql sink failed", err)
		}
		err = consumer.Start(context.Background(), sink)
		if err != nil {
			log.Fatal("start kafka consumer failed", err)
		}
		return nil
	},
}
