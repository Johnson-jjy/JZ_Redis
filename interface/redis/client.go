package redis

// Connection represents a connection with redis Client
type Connection interface {
	Write([]byte) error
	SetPassword(string)
	GetPassword(string)

	// client should keep its subscribing channels
	Subscribe(channel string)
	UnSubscribe(channel string)
	SubsCount() int
	GetChannels() []string

	// used for `Multi` command
	InMultiState() bool
	SetMultiState(bool)
	GetQueuedCmdLine() [][][]byte
	EnqueueCmd([][]byte)
	ClearQueueCmds()
	GetWatching() map[string]uint32
}
