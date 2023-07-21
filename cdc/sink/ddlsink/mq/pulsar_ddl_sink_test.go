package mq

import (
	"context"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink/mq/ddlproducer"
	"github.com/pingcap/tiflow/pkg/config"
	"net/url"
	"testing"
)

func TestNewPulsarDDLSink(t *testing.T) {
	type args struct {
		ctx             context.Context
		sinkURI         *url.URL
		replicaConfig   *config.ReplicaConfig
		producerCreator ddlproducer.Factory
	}
	tests := []struct {
		name    string
		args    args
		want    *ddlSink
		wantErr bool
	}{
		{
			name: "test normal new pulsar ddl sink",
			args: args{
				ctx:             context.Background(),
				sinkURI:         &url.URL{},
				replicaConfig:   &config.ReplicaConfig{},
				producerCreator: nil,
			},
			want:    &ddlSink{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewPulsarDDLSink(tt.args.ctx, tt.args.sinkURI,
				tt.args.replicaConfig, ddlproducer.NewPulsarProducer)
			if err != nil {
				t.Errorf("NewPulsarDDLSink() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			t.Logf("got sink = %+v", got)

		})
	}
}
