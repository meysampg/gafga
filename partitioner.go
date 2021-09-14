package gafga

import "sync"

type Partitioner interface {
	GetLastProducedPartition(msg Message) int
	GetLastConsumedPartition(msg Message) int
	UpsetLastProducedPartition(msg Message) int
	UpsetLastConsumedPartition(msg Message) int
}

type RoundRobinPartitioner struct {
	partitionNumbers int

	lastProducedPartition int
	lastConsumedPartition int

	pMu *sync.RWMutex
	cMu *sync.RWMutex
}

func NewRoundRobinPartitioner(partitionNumbers int) *RoundRobinPartitioner {
	return &RoundRobinPartitioner{
		partitionNumbers: partitionNumbers,
		pMu:              new(sync.RWMutex),
		cMu:              new(sync.RWMutex),
	}
}

func (r *RoundRobinPartitioner) GetLastProducedPartition(msg Message) int {
	r.pMu.RLock()
	defer r.pMu.RUnlock()
	return r.lastProducedPartition
}

func (r *RoundRobinPartitioner) GetLastConsumedPartition(msg Message) int {
	r.cMu.RLock()
	defer r.cMu.RUnlock()
	return r.lastConsumedPartition
}

func (r *RoundRobinPartitioner) UpsetLastProducedPartition(msg Message) int {
	r.pMu.Lock()
	defer r.pMu.Unlock()

	last := r.lastProducedPartition
	r.lastProducedPartition = (r.lastProducedPartition + 1) % r.partitionNumbers

	return last
}

func (r *RoundRobinPartitioner) UpsetLastConsumedPartition(msg Message) int {
	r.cMu.Lock()
	defer r.cMu.Unlock()

	last := r.lastConsumedPartition
	r.lastConsumedPartition = (r.lastConsumedPartition + 1) % r.partitionNumbers

	return last
}
