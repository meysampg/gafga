package gafga

type PartitionError string

func (p PartitionError) Error() string {
	return string(p)
}

const (
	EmptyPartitionError          PartitionError = "partition is empty"
	UninitializedPartitionError  PartitionError = "partition is not initialized"
	CleanedMessagePartitionError PartitionError = "requested message is cleaned"
)

type ReadOffset int

const (
	ReadLatestOnPartition   ReadOffset = -1
	ReadEarliestInPartition ReadOffset = 0
)

type Partition interface {
	Append(msg Message) error
	Read(from ReadOffset) (Message, error)
}

type InMemoryPartition struct {
	data []Message
}

func NewInMemoryPartition() *InMemoryPartition {
	return &InMemoryPartition{
		data: make([]Message, 0),
	}
}

func (p *InMemoryPartition) Append(msg Message) error {
	if p.data == nil {
		return UninitializedPartitionError
	}

	p.data = append(p.data, msg)

	return nil
}

func (p *InMemoryPartition) Read(from ReadOffset) (Message, error) {
	if p.data == nil {
		return Message{}, UninitializedPartitionError
	}

	if len(p.data) == 0 {
		return Message{}, EmptyPartitionError
	}

	msg := p.data[0]
	p.data = p.data[1:]

	return msg, nil
}
