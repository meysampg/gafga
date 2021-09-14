package gafga

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRoundRobinPartitioner_Produce_NonConcurrentMode(t *testing.T) {
	type fields struct {
		partitionNumbers int
		repeatCount      int
	}
	type want struct {
		partitionReadyToProduce int
		producedPartitions      []int
	}
	msg := Message{}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "Fresh partitioner must return zero",
			fields: fields{
				partitionNumbers: 1,
				repeatCount:      0,
			},
			want: want{
				partitionReadyToProduce: 0,
				producedPartitions:      []int{},
			},
		},
		{
			name: "Single Partition always return same value",
			fields: fields{
				partitionNumbers: 1,
				repeatCount:      5,
			},
			want: want{
				partitionReadyToProduce: 0,
				producedPartitions:      []int{0, 0, 0, 0, 0},
			},
		},
		{
			name: "Multiple Partition always return zero on first iteration",
			fields: fields{
				partitionNumbers: 5,
				repeatCount:      1,
			},
			want: want{
				partitionReadyToProduce: 1,
				producedPartitions:      []int{0},
			},
		},
		{
			name: "Multiple Partition, multiple run partitionNumbers > repeatCount",
			fields: fields{
				partitionNumbers: 4,
				repeatCount:      3,
			},
			want: want{
				partitionReadyToProduce: 3,
				producedPartitions:      []int{0, 1, 2},
			},
		},
		{
			name: "Multiple Partition, multiple run partitionNumbers <= repeatCount",
			fields: fields{
				partitionNumbers: 5,
				repeatCount:      16,
			},
			want: want{
				partitionReadyToProduce: 1,
				producedPartitions:      []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRoundRobinPartitioner(tt.fields.partitionNumbers)
			got := make([]int, tt.fields.repeatCount)
			for i := 0; i < tt.fields.repeatCount; i++ {
				got[i] = r.UpsetLastProducedPartition(msg)
			}
			if !assert.ElementsMatch(t, got, tt.want.producedPartitions) {
				t.Errorf("UpsetLastProducedPartition() = %v, want %v", got, tt.want.producedPartitions)
			}
			if got := r.GetLastProducedPartition(msg); got != tt.want.partitionReadyToProduce {
				t.Errorf("GetLastProducedPartition() = %v, want %v", got, tt.want.partitionReadyToProduce)
			}
		})
	}
}

func TestRoundRobinPartitioner_Produce_ConcurrentMode(t *testing.T) {
	type fields struct {
		partitionNumbers int
		repeatCount      int
	}
	type want struct {
		partitionReadyToProduce int
		producedPartitions      []int
	}
	msg := Message{}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "Fresh partitioner must return zero",
			fields: fields{
				partitionNumbers: 1,
				repeatCount:      0,
			},
			want: want{
				partitionReadyToProduce: 0,
				producedPartitions:      []int{},
			},
		},
		{
			name: "Single Partition always return same value",
			fields: fields{
				partitionNumbers: 1,
				repeatCount:      5,
			},
			want: want{
				partitionReadyToProduce: 0,
				producedPartitions:      []int{0, 0, 0, 0, 0},
			},
		},
		{
			name: "Multiple Partition always return zero on first iteration",
			fields: fields{
				partitionNumbers: 5,
				repeatCount:      1,
			},
			want: want{
				partitionReadyToProduce: 1,
				producedPartitions:      []int{0},
			},
		},
		{
			name: "Multiple Partition, multiple run partitionNumbers > repeatCount",
			fields: fields{
				partitionNumbers: 4,
				repeatCount:      3,
			},
			want: want{
				partitionReadyToProduce: 3,
				producedPartitions:      []int{0, 1, 2},
			},
		},
		{
			name: "Multiple Partition, multiple run partitionNumbers <= repeatCount",
			fields: fields{
				partitionNumbers: 5,
				repeatCount:      16,
			},
			want: want{
				partitionReadyToProduce: 1,
				producedPartitions:      []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRoundRobinPartitioner(tt.fields.partitionNumbers)
			got := runUpsetsConcurrent(tt.fields.repeatCount, r.UpsetLastProducedPartition)
			if !assert.ElementsMatch(t, got, tt.want.producedPartitions) {
				t.Errorf("UpsetLastProducedPartition() = %v, want %v", got, tt.want.producedPartitions)
			}
			if got := r.GetLastProducedPartition(msg); got != tt.want.partitionReadyToProduce {
				t.Errorf("GetLastProducedPartition() = %v, want %v", got, tt.want.partitionReadyToProduce)
			}
		})
	}
}

func TestRoundRobinPartitioner_Consume_NonConcurrentMode(t *testing.T) {
	type fields struct {
		partitionNumbers int
		repeatCount      int
	}
	type want struct {
		partitionReadyToConsume int
		consumedPartitions      []int
	}
	msg := Message{}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "Fresh partitioner must return zero",
			fields: fields{
				partitionNumbers: 1,
				repeatCount:      0,
			},
			want: want{
				partitionReadyToConsume: 0,
				consumedPartitions:      []int{},
			},
		},
		{
			name: "Single Partition always return same value",
			fields: fields{
				partitionNumbers: 1,
				repeatCount:      5,
			},
			want: want{
				partitionReadyToConsume: 0,
				consumedPartitions:      []int{0, 0, 0, 0, 0},
			},
		},
		{
			name: "Multiple Partition always return zero on first iteration",
			fields: fields{
				partitionNumbers: 5,
				repeatCount:      1,
			},
			want: want{
				partitionReadyToConsume: 1,
				consumedPartitions:      []int{0},
			},
		},
		{
			name: "Multiple Partition, multiple run partitionNumbers > repeatCount",
			fields: fields{
				partitionNumbers: 4,
				repeatCount:      3,
			},
			want: want{
				partitionReadyToConsume: 3,
				consumedPartitions:      []int{0, 1, 2},
			},
		},
		{
			name: "Multiple Partition, multiple run partitionNumbers <= repeatCount",
			fields: fields{
				partitionNumbers: 5,
				repeatCount:      16,
			},
			want: want{
				partitionReadyToConsume: 1,
				consumedPartitions:      []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRoundRobinPartitioner(tt.fields.partitionNumbers)
			got := make([]int, tt.fields.repeatCount)
			for i := 0; i < tt.fields.repeatCount; i++ {
				got[i] = r.UpsetLastConsumedPartition(msg)
			}
			if !assert.ElementsMatch(t, got, tt.want.consumedPartitions) {
				t.Errorf("UpsetLastConsumedPartition() = %v, want %v", got, tt.want.consumedPartitions)
			}
			if got := r.GetLastConsumedPartition(msg); got != tt.want.partitionReadyToConsume {
				t.Errorf("GetLastConsumedPartition() = %v, want %v", got, tt.want.partitionReadyToConsume)
			}
		})
	}
}

func TestRoundRobinPartitioner_Consume_ConcurrentMode(t *testing.T) {
	type fields struct {
		partitionNumbers int
		repeatCount      int
	}
	type want struct {
		partitionReadyToConsume int
		consumedPartitions      []int
	}
	msg := Message{}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "Fresh partitioner must return zero",
			fields: fields{
				partitionNumbers: 1,
				repeatCount:      0,
			},
			want: want{
				partitionReadyToConsume: 0,
				consumedPartitions:      []int{},
			},
		},
		{
			name: "Single Partition always return same value",
			fields: fields{
				partitionNumbers: 1,
				repeatCount:      5,
			},
			want: want{
				partitionReadyToConsume: 0,
				consumedPartitions:      []int{0, 0, 0, 0, 0},
			},
		},
		{
			name: "Multiple Partition always return zero on first iteration",
			fields: fields{
				partitionNumbers: 5,
				repeatCount:      1,
			},
			want: want{
				partitionReadyToConsume: 1,
				consumedPartitions:      []int{0},
			},
		},
		{
			name: "Multiple Partition, multiple run partitionNumbers > repeatCount",
			fields: fields{
				partitionNumbers: 4,
				repeatCount:      3,
			},
			want: want{
				partitionReadyToConsume: 3,
				consumedPartitions:      []int{0, 1, 2},
			},
		},
		{
			name: "Multiple Partition, multiple run partitionNumbers <= repeatCount",
			fields: fields{
				partitionNumbers: 5,
				repeatCount:      16,
			},
			want: want{
				partitionReadyToConsume: 1,
				consumedPartitions:      []int{0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRoundRobinPartitioner(tt.fields.partitionNumbers)
			got := runUpsetsConcurrent(tt.fields.repeatCount, r.UpsetLastConsumedPartition)
			if !assert.ElementsMatch(t, got, tt.want.consumedPartitions) {
				t.Errorf("UpsetLastConsumedPartition() = %v, want %v", got, tt.want.consumedPartitions)
			}
			if got := r.GetLastConsumedPartition(msg); got != tt.want.partitionReadyToConsume {
				t.Errorf("GetLastConsumedPartition() = %v, want %v", got, tt.want.partitionReadyToConsume)
			}
		})
	}
}

func runUpsetsConcurrent(count int, upsetHandler func(Message) int) []int {
	partitions := make(chan int, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			partitions <- upsetHandler(Message{})
		}()
	}
	go func() {
		wg.Wait()
		close(partitions)
	}()

	result := make([]int, 0)
	for i := range partitions {
		result = append(result, i)
	}

	return result
}
