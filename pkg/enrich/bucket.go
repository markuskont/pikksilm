package enrich

import (
	"errors"
	"fmt"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
)

type BucketHandlerFunc func(*Bucket) error
type ContainerCreateFunc func() any

type WinlogData struct {
	// NetworkEvents is a list of 5-tuples + GUID we cache in case they were seen before the command
	NetworkEvents []models.NetworkEntry
	// CommandEvents is a lookuptable of GUID to event_id 1 entry, assuming we only see one cmd per GUID
	CommandEvents map[string]models.Entry
}

type Bucket struct {
	Data any
	Time time.Time
}

func NewBucket(ts time.Time, fn ContainerCreateFunc) *Bucket {
	b := &Bucket{Time: ts}
	if fn != nil {
		b.Data = fn()
	}
	return b
}

type Buckets struct {
	Buckets []Bucket

	size  time.Duration
	count int

	first   time.Time
	current time.Time

	ccFunc ContainerCreateFunc
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
		b.Buckets = append(b.Buckets, *NewBucket(b.current, b.ccFunc))
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

type bucketsConfig struct {
	// TODO - would be more intuitive if this was actuall calculated from global duration
	Count int
	// Size for single bucket
	Size time.Duration
	// ContainerCreateFunc can be nil, in that case user is responsible for checking that b.Data is not nil
	ContainerCreateFunc ContainerCreateFunc
}

func newBuckets(c bucketsConfig) (*Buckets, error) {
	if c.Count < 1 {
		return nil, fmt.Errorf("invalid bucket count %d", c.Count)
	}
	if c.Size == 0 {
		return nil, errors.New("empty bucket size")
	}
	return &Buckets{
		Buckets: make([]Bucket, 0, c.Count),
		size:    c.Size,
		count:   c.Count,
		ccFunc:  c.ContainerCreateFunc,
	}, nil
}
