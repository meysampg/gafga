Abaji Gafga®
===========
Abaji Gafga® is an examining to implement Apache Kafka with Golang. Initially, it was a sample code to describe `sync.Cond` mechanism in Golang, but meanwhile, it's changed the direction from that to implementing a Kafka-like broker in Golang.

The first release of this repository remains for the primary goal (implementing with `sync.Cond`), but I think it's a tremendous challenge to make it distributed.

## TODO
 - [ ] Add support of WAL
 - [ ] Add consumer group support
 - [ ] Make topics replicated across a cluster
 - [ ] Implement an Anti-entropy mechanism
 - [ ] Implement a failure detection mechanism
 - [x] Add support of different partitioners
 - [ ] Leader election for each partition of a topic
 - [ ] Add support of change in the number of partitions
 - [ ] Implement a leader-aware read mechanism
