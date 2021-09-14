package gafga

import (
	"sync"
)

type Topic struct {
	partitions []Partition

	partitionConds []*sync.Cond

	partitioner Partitioner
}

func (t *Topic) Produce(msg Message) error {
	partition := t.partitioner.UpsetLastProducedPartition(msg)

	return t.partitions[partition].Append(msg)
}

func (t *Topic) Consume(topic string) (Message, error) {
	partition := t.partitioner.UpsetLastProducedPartition(msg)

	return t.partitions[partition].Append(msg)
}
