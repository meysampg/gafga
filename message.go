package gafga

// Message contains a produced message and its related meta information. It
// should be immutable so for avoiding sudden modification, it must be passed
// as value. YEP, Key and Msg are slices but for now it's okay.
type Message struct {
	Key []byte
	Msg []byte

	Offset    int
	Partition int
	Topic     string
}
