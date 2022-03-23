package enrich

import (
	"errors"
	"fmt"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
)

type BucketHandlerFunc func(*Bucket) error

type Bucket struct {
	// event_id 3 cache
	NetworkEvents []models.NetworkEntry
	// Beginning of this bucket
	Time time.Time
}

func NewBucket(ts time.Time) *Bucket {
	return &Bucket{
		Time:          ts,
		NetworkEvents: make([]models.NetworkEntry, 0),
	}
}

type Buckets struct {
	Buckets []Bucket

	size  time.Duration
	count int

	first   time.Time
	current time.Time
}

func (b *Buckets) Insert(fn BucketHandlerFunc) error {
	b.tryRotate(time.Now())
	return fn(&b.Buckets[len(b.Buckets)-1])
}

func (b *Buckets) Check(fn BucketHandlerFunc, window time.Duration) error {
	ts := time.Now()
	for i, bucket := range b.Buckets {
		if end := bucket.Time.Add(b.size); end.After(ts.Add(-1 * window)) {
			if err := fn(&b.Buckets[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Buckets) tryRotate(ts time.Time) {
	// append new bucket in case period is exceeded
	if b.current.IsZero() || b.rollover(ts) {
		b.current = ts.Truncate(b.size)
		b.first = b.current
		b.Buckets = append(b.Buckets, *NewBucket(b.current))
	}
	// truncate bucket set in case current count is above max
	if current := len(b.Buckets); current > 1 && current > b.count {
		b.Buckets = b.Buckets[1:]
	}
}

func (b Buckets) rollover(ts time.Time) bool {
	since := ts.Sub(b.current)
	return since > b.size
}

func NewBuckets(count int, size time.Duration) (*Buckets, error) {
	if count < 1 {
		return nil, fmt.Errorf("invalid bucket count %d", count)
	}
	if size == 0 {
		return nil, errors.New("empty bucket size")
	}
	return &Buckets{
		Buckets: make([]Bucket, 0, count),
		size:    size,
		count:   count,
	}, nil
}
