package etcdkv

import (
	"strconv"

	"github.com/pingcap/tiflow/pkg/errorutil"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/meta/metaclient"
)

func makePutResp(etcdResp *clientv3.PutResponse) *metaclient.PutResponse {
	resp := &metaclient.PutResponse{
		Header: &metaclient.ResponseHeader{
			// [TODO] use another ClusterID
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
	}

	return resp
}

func makeGetResp(etcdResp *clientv3.GetResponse) *metaclient.GetResponse {
	kvs := make([]*metaclient.KeyValue, 0, len(etcdResp.Kvs))
	for _, kv := range etcdResp.Kvs {
		kvs = append(kvs, &metaclient.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}
	resp := &metaclient.GetResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
		Kvs: kvs,
	}

	return resp
}

func makeDeleteResp(etcdResp *clientv3.DeleteResponse) *metaclient.DeleteResponse {
	resp := &metaclient.DeleteResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
	}

	return resp
}

func makeTxnResp(etcdResp *clientv3.TxnResponse) *metaclient.TxnResponse {
	rsps := make([]metaclient.ResponseOp, 0, len(etcdResp.Responses))
	for _, eRsp := range etcdResp.Responses {
		switch eRsp.Response.(type) {
		case *etcdserverpb.ResponseOp_ResponseRange:
			getRsp := makeGetResp((*clientv3.GetResponse)(eRsp.GetResponseRange()))
			rsps = append(rsps, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponseGet{
					ResponseGet: getRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponsePut:
			putRsp := makePutResp((*clientv3.PutResponse)(eRsp.GetResponsePut()))
			rsps = append(rsps, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponsePut{
					ResponsePut: putRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponseDeleteRange:
			delRsp := makeDeleteResp((*clientv3.DeleteResponse)(eRsp.GetResponseDeleteRange()))
			rsps = append(rsps, metaclient.ResponseOp{
				Response: &metaclient.ResponseOpResponseDelete{
					ResponseDelete: delRsp,
				},
			})
		case *etcdserverpb.ResponseOp_ResponseTxn:
			panic("unexpected nested txn")
		}
	}

	return &metaclient.TxnResponse{
		Header: &metaclient.ResponseHeader{
			ClusterID: strconv.FormatUint(etcdResp.Header.ClusterId, 10),
		},
		Responses: rsps,
	}
}

// etcdError wraps IsRetryable to etcd error.
type etcdError struct {
	displayed error
	cause     error
}

func (e *etcdError) IsRetryable() bool {
	if e.cause != nil {
		return errorutil.IsRetryableEtcdError(e.cause)
	}
	// currently all retryable errors are etcd errors
	return false
}

func (e *etcdError) Error() string {
	return e.displayed.Error()
}

func etcdErrorFromOpFail(err error) *etcdError {
	return &etcdError{
		cause:     err,
		displayed: cerrors.ErrMetaOpFail.GenWithStackByArgs(err),
	}
}
