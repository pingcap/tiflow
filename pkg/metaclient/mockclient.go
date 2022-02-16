package metaclient

import "context"

type mockKVClient struct {
	endpoint string
	// [TODO]
}

func NewMockKVClient(endpoint string) KVClient {
	return &mockKVClient{endpoint}
}

func (m *mockKVClient) Put(ctx context.Context, key, val string) (*PutResponse, error) {
	// [TODO]
	return nil, nil
}

func (m *mockKVClient) Get(ctx context.Context, key string, opts ...OpOption) (*GetResponse, error) {
	// [TODO]
	return nil, nil
}

func (m *mockKVClient) Delete(ctx context.Context, key string, opts ...OpOption) (*DeleteResponse, error) {
	// [TODO]
	return nil, nil
}

func (m *mockKVClient) Do(ctx context.Context, op Op) (OpResponse, error) {
	// [TODO]
	return OpResponse{}, nil
}

func (m *mockKVClient) Txn(ctx context.Context) Txn {
	// [TODO]
	return &mockTxn{}
}

func (m *mockKVClient) Close() error {
	// [TODO]
	return nil
}

type mockTxn struct { // [TODO]
}

func (txn *mockTxn) Do(ops ...Op) Txn {
	// [TODO]
	return txn
}

func (txn *mockTxn) Commit() (*TxnResponse, error) {
	// [TODO]
	return nil, nil
}
