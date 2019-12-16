package cmd

import (
	"context"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/google/uuid"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

func init() {
	rootCmd.AddCommand(cliCmd)

	cliCmd.Flags().StringVar(&pdAddress, "pd-addr", "localhost:2379", "address of PD")
	cliCmd.Flags().Uint64Var(&startTs, "start-ts", 0, "start ts of changefeed")
	cliCmd.Flags().StringVar(&sinkURI, "sink-uri", "root@tcp(127.0.0.1:3306)/test", "sink uri")
	cliCmd.Flags().BoolVar(&sinkToKafka, "sink-to-kafka", false, "whether sink to kafka")
	cliCmd.Flags().StringVar(&kafkaAddress, "sink-kafka-address", "localhost:9092", "kafka address")
	cliCmd.Flags().StringVar(&kafkaVer, "sink-kafka-version", "2.0.0", "kafka version")
	cliCmd.Flags().IntVar(&kafkaMaxMessageBytes, "sink-kafka-max-message-bytes", 0, "kafka max message bytes")
	cliCmd.Flags().StringVar(&kafkaTopicName, "sink-kafka-topic", "", "kafka topic")
}

var (
	pdAddress            string
	startTs              uint64
	sinkURI              string
	sinkToKafka          bool
	kafkaAddress         string
	kafkaVer             string
	kafkaMaxMessageBytes int
	kafkaTopicName          string
)

var cliCmd = &cobra.Command{
	Use:   "cli",
	Short: "simulate client to create changefeed",
	Long:  ``,
	RunE: func(cmd *cobra.Command, args []string) error {
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
		id := uuid.New().String()
		detail := &model.ChangeFeedDetail{
			SinkURI:         sinkURI,
			SinkToKafka:     sinkToKafka,
			KafkaAddress:    kafkaAddress,
			KafkaVersion:    kafkaVer,
			KafkaMaxMessage: kafkaMaxMessageBytes,
			KafkaTopic:      kafkaTopicName,
			Opts:            make(map[string]string),
			CreateTime:      time.Now(),
			StartTs:         startTs,
		}
		fmt.Printf("create changefeed detail %+v\n", detail)
		return kv.SaveChangeFeedDetail(context.Background(), cli, detail, id)
	},
}
