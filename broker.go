package gafga

type Broker struct {
	defaultPartitionNumbers int

	topics map[string]*Topic
}

func New(partitionNumbers int) *Broker {
	return &Broker{
		defaultPartitionNumbers: partitionNumbers,
		topics:                  make(map[string]*Topic),
	}
}

func (b *Broker) Produce(topic string, msg Message) {

}

func (b *Broker) Consume(topic string) Message {
	return Message{}
}
