package cloudstorage

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/builder"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/util"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func testEncodingWorker(ctx context.Context, t *testing.T) *encodingWorker {
	uri := fmt.Sprintf("file:///%s", t.TempDir())
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)
	encoderConfig, err := util.GetEncoderConfig(sinkURI, config.ProtocolOpen,
		config.GetDefaultReplicaConfig(), config.DefaultMaxMessageBytes)
	require.Nil(t, err)
	encoderBuilder, err := builder.NewEventBatchEncoderBuilder(context.TODO(), encoderConfig)
	require.Nil(t, err)
	encoder := encoderBuilder.Build()
	errCh := make(chan error, 10)
	changefeedID := model.DefaultChangeFeedID("test-encode")

	bs, err := storage.ParseBackend(uri, &storage.BackendOptions{})
	require.Nil(t, err)
	storage, err := storage.New(ctx, bs, nil)
	require.Nil(t, err)

	dmlWriter := newDMLWriter(ctx, changefeedID, storage, 1, ".json", errCh)
	worker := newEncodingWorker(1, changefeedID, encoder, dmlWriter, errCh)
	return worker
}

func TestEncodeEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	worker := testEncodingWorker(ctx, t)
	err := worker.encodeEvents(ctx, eventFragment{
		tableName: model.TableName{
			Schema:  "test",
			Table:   "table1",
			TableID: 100,
		},
		seqNumber: 1,
		event: &eventsink.TxnCallbackableEvent{
			Event: &model.SingleTableTxn{
				Table: &model.TableName{
					Schema:  "test",
					Table:   "table1",
					TableID: 100,
				},
				Rows: []*model.RowChangedEvent{
					{
						Table: &model.TableName{
							Schema:  "test",
							Table:   "table1",
							TableID: 100,
						},
						Columns: []*model.Column{
							{Name: "c1", Value: 100},
							{Name: "c2", Value: "hello world"},
						},
					},
					{
						Table: &model.TableName{
							Schema:  "test",
							Table:   "table1",
							TableID: 100,
						},
						Columns: []*model.Column{
							{Name: "c1", Value: 200},
							{Name: "c2", Value: "你好，世界"},
						},
					},
				},
			},
		},
	})
	require.Nil(t, err)
	cancel()

	worker.writer.stop()
}

func TestWorkerRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	worker := testEncodingWorker(ctx, t)
	msgCh := chann.New[eventFragment]()
	worker.run(ctx, msgCh)
	table := model.TableName{
		Schema:  "test",
		Table:   "table1",
		TableID: 100,
	}
	event := &model.SingleTableTxn{
		Table: &model.TableName{
			Schema:  "test",
			Table:   "table1",
			TableID: 100,
		},
		Rows: []*model.RowChangedEvent{
			{
				Table: &model.TableName{
					Schema:  "test",
					Table:   "table1",
					TableID: 100,
				},
				Columns: []*model.Column{
					{Name: "c1", Value: 100},
					{Name: "c2", Value: "hello world"},
				},
			},
		},
	}

	for i := 0; i < 3; i++ {
		frag := eventFragment{
			tableName: table,
			seqNumber: uint64(i + 1),
			event: &eventsink.TxnCallbackableEvent{
				Event: event,
			},
		}
		msgCh.In() <- frag
	}
	msgCh.In() <- eventFragment{
		seqNumber: 3,
		tableName: table,
	}
	cancel()
	worker.stop()
	worker.writer.stop()
}
