package metaclient

type ClientType int

const (
	TypeKVClient ClientType = iota /* KV client style，like etcd/consul/TiKV/redis or even SQL backend*/
)

type KVClient interface {
	KV
	Close()
}
