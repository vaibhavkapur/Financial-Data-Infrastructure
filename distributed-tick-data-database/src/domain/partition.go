package domain

import (
	"fmt"
	"hash/fnv"
	"time"
)

// Partitioner computes deterministic shard assignments and UTC time bucket keys.
type Partitioner struct {
	NumShards   int
	Granularity string // "hour" | "day"
}

func NewPartitioner(numShards int, granularity string) Partitioner {
	if numShards < 1 {
		numShards = 1
	}
	if granularity != "day" {
		granularity = "hour"
	}
	return Partitioner{
		NumShards:   numShards,
		Granularity: granularity,
	}
}

// ShardForInstrument uses deterministic FNV-1a-32 hashing so all events for a
// given instrument land on the same shard.
func (p Partitioner) ShardForInstrument(instrumentID string) uint16 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(instrumentID))
	return uint16(h.Sum32() % uint32(p.NumShards))
}

// TimeBucket formats the UTC time partition prefix from event_time_ns.
func (p Partitioner) TimeBucket(eventTimeNS int64) string {
	t := time.Unix(0, eventTimeNS).UTC()
	if p.Granularity == "day" {
		return t.Format("2006-01-02")
	}
	return t.Format("2006-01-02T15")
}

// PartitionKey returns the canonical partition path key "<time_bucket>/shard-<NN>".
func (p Partitioner) PartitionKey(eventTimeNS int64, shardID uint16) string {
	return fmt.Sprintf("%s/shard-%02d", p.TimeBucket(eventTimeNS), shardID)
}

// TimeBucketsInRange returns all UTC time buckets intersecting [startNS, endNS] inclusive.
func (p Partitioner) TimeBucketsInRange(startNS, endNS int64) []string {
	if endNS < startNS {
		return nil
	}
	start := time.Unix(0, startNS).UTC()
	end := time.Unix(0, endNS).UTC()

	var step time.Duration
	var cursor time.Time
	if p.Granularity == "day" {
		step = 24 * time.Hour
		cursor = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, time.UTC)
	} else {
		step = time.Hour
		cursor = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), 0, 0, 0, time.UTC)
	}

	var buckets []string
	for !cursor.After(end) {
		if p.Granularity == "day" {
			buckets = append(buckets, cursor.Format("2006-01-02"))
		} else {
			buckets = append(buckets, cursor.Format("2006-01-02T15"))
		}
		cursor = cursor.Add(step)
	}
	return buckets
}
