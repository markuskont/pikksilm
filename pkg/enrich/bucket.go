package enrich

import (
	"errors"
	"fmt"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
)

type BucketHandlerFunc func(*Bucket) error
type ContainerCreateFunc func() any

type NetworkEvents []models.NetworkEntry
type CommandEvents map[string]models.Entry

type winlogBuckets struct {
	network  *Buckets
	commands *Buckets
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

func (b *Buckets) insert(fn BucketHandlerFunc, ts time.Time) error {
	b.tryRotate(ts)
	return fn(&b.Buckets[len(b.Buckets)-1])
}

func (b *Buckets) InsertCurrent(fn BucketHandlerFunc) error {
	return b.insert(fn, time.Now())
}

func (b *Buckets) check(fn BucketHandlerFunc, ts time.Time) error {
	for i := range b.Buckets {
		if err := fn(&b.Buckets[i]); err != nil {
			return err
		}
	}
	return nil
}

func (b *Buckets) Check(fn BucketHandlerFunc) error {
	return b.check(fn, time.Now())
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

// BucketsConfig is public config object to be exposed to users
type BucketsConfig struct {
	// TODO - would be more intuitive if this was actually calculated from global duration
	Count int
	Size  time.Duration
}

// private config with params not yet meant to be exposed (such as function)
type bucketsConfig struct {
	BucketsConfig
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
