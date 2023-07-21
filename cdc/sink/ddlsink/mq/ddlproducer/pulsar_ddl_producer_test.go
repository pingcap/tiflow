package ddlproducer

import (
	"context"
	"fmt"
	"math/rand"
	url2 "net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pingcap/tiflow/cdc/model"
	pulsarMetric "github.com/pingcap/tiflow/cdc/sink/metrics/mq/pulsar"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	pulsarConfig "github.com/pingcap/tiflow/pkg/sink/pulsar"
)

func newPulsarConfig() *pulsarConfig.PulsarConfig {
	sinkUrl := os.Getenv("sink-uri")
	fmt.Println(sinkUrl)
	if len(sinkUrl) <= 0 {
		panic("sink-uri is empty: " + sinkUrl)
	}
	u := &url2.URL{}
	u, err := u.Parse(sinkUrl)
	if err != nil {
		panic(err)
	}
	c := &pulsarConfig.PulsarConfig{}
	err = c.Apply(u)
	if err != nil {
		panic(err)
	}
	return c
}

func TestNewPulsarProducer(t *testing.T) {
	config := newPulsarConfig()
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            config.URL,
		Authentication: pulsar.NewAuthenticationToken(config.AuthenticationToken),
	})
	if err != nil {
		t.Errorf("new client fail %+v", err)
		return
	}

	ctx, fc := context.WithTimeout(context.Background(), time.Second*5)

	type args struct {
		ctx          context.Context
		client       pulsar.Client
		pulsarConfig *pulsarConfig.PulsarConfig
		errCh        chan error
	}
	tests := []struct {
		name    string
		args    args
		want    DDLProducer
		wantErr bool
	}{
		{
			name: "New",
			args: args{
				ctx:          ctx,
				client:       client,
				pulsarConfig: config,
				errCh:        make(chan error),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer, err := NewPulsarProducer(tt.args.ctx, tt.args.pulsarConfig, tt.args.client)
			if err != nil {
				t.Errorf("NewPulsarDMLProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer producer.Close()

			select {
			case e := <-tt.args.errCh:
				t.Logf("errChan %+v", e)
			case <-ctx.Done():
				fc()
				t.Logf("Done")
			}
		})
	}
}

func Test_pulsarProducers_SyncSendMessage(t *testing.T) {

	config := newPulsarConfig()
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:            config.URL,
		Authentication: pulsar.NewAuthenticationToken(config.AuthenticationToken),
	})
	if err != nil {
		t.Errorf("new client fail %+v", err)
		return
	}

	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)

	type args struct {
		ctx          context.Context
		topic        string
		partition    int32
		message      *common.Message
		client       pulsar.Client
		pulsarConfig *pulsarConfig.PulsarConfig
		errCh        chan error
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "New pulsar client and AsyncSendMessage",
			args: args{
				ctx:       ctx,
				topic:     "test",
				partition: 1,
				message: &common.Message{
					Key:   []byte("key"),
					Value: []byte("this value for test input data"),
					Callback: func() {
						fmt.Println("callback: message send success!")
					},
				},
				client:       client,
				pulsarConfig: config,
				errCh:        make(chan error),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := NewPulsarProducer(tt.args.ctx, tt.args.pulsarConfig, tt.args.client)
			if err != nil {
				t.Errorf("NewPulsarDMLProducer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			//defer p.Close()
			if err := p.SyncSendMessage(tt.args.ctx, tt.args.topic, tt.args.partition, tt.args.message); err != nil {
				t.Errorf("AsyncSendMessage() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(time.Second * 1)
			p.Close()
			client.Close()
		})
	}

}

func TestDDLIncreaseMetric(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	index := 0
	m := &common.Message{
		Type: model.MessageTypeDDL,
	}

	for {
		x := rand.Int31() % 8
		time.Sleep(time.Second * time.Duration(x))
		if index%3 == 1 {
			pulsarMetric.IncPublishedDDLEventCountMetricSuccess("testtopic",
				"test", m)
		}
		pulsarMetric.IncPublishedDDLEventCountMetric("testtopic",
			"test", m)

		pulsarMetric.IncPublishedDDLEventCountMetric("testtopic",
			"test", m)

		pulsarMetric.IncPublishedDDLEventCountMetric("noTable",
			"test", m)

		pulsarMetric.IncPublishedDDLEventCountMetric("noSchema",
			"test", m)

		index++
	}

}

func Test_pulsarProducers_closeProducersMapByTopic(t *testing.T) {
	type fields struct {
		client           pulsar.Client
		pConfig          *pulsarConfig.PulsarConfig
		defaultTopicName string
		producers        map[string]pulsar.Producer
		producersMutex   sync.RWMutex
		changefeedID     model.ChangeFeedID
	}
	type args struct {
		topicName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &pulsarProducers{
				client:           tt.fields.client,
				pConfig:          tt.fields.pConfig,
				defaultTopicName: tt.fields.defaultTopicName,
				producers:        tt.fields.producers,
				producersMutex:   tt.fields.producersMutex,
				id:               tt.fields.changefeedID,
			}
			if err := p.closeProducersMapByTopic(tt.args.topicName); (err != nil) != tt.wantErr {
				t.Errorf("closeProducersMapByTopic() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
