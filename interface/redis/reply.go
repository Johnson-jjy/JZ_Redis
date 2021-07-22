package redis


// Reply is the interface of redis serialization protocl message
type Reply interface {
	ToBytes() []byte
}
