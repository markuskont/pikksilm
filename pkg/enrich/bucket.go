package enrich

import (
	"compress/gzip"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
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
	Size    time.Duration
	Count   int
	First   time.Time
	Current time.Time

	ccFunc ContainerCreateFunc
}

func (b *Buckets) setCCFunc(fn ContainerCreateFunc) { b.ccFunc = fn }

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
	if b.Current.IsZero() || b.rollover(ts) {
		b.Current = ts.Truncate(b.Size)
		b.First = b.Current
		b.Buckets = append(b.Buckets, *NewBucket(b.Current, b.ccFunc))
	}
	// truncate bucket set in case current count is above max
	if current := len(b.Buckets); current > 1 && current > b.Count {
		b.Buckets = b.Buckets[1:]
	}
}

func (b Buckets) rollover(ts time.Time) bool {
	since := ts.Sub(b.Current)
	return since > b.Size
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
	// ContainerCreateFunc can be empty value,
	// in that case user is responsible for checking that b.Data is not nil
	ContainerCreateFunc ContainerCreateFunc

	Persist string
}

func newBuckets(c bucketsConfig) (*Buckets, error) {
	if c.Count < 1 {
		return nil, fmt.Errorf("invalid bucket count %d", c.Count)
	}
	if c.Size == 0 {
		return nil, errors.New("empty bucket size")
	}
	if c.Persist != "" {
		_, err := os.Stat(c.Persist)
		if !errors.Is(err, os.ErrNotExist) {
			return readBucketPersist(c.Persist, c.ContainerCreateFunc)
		}
	}
	return &Buckets{
		Buckets: make([]Bucket, 0, c.Count),
		Size:    c.Size,
		Count:   c.Count,
		ccFunc:  c.ContainerCreateFunc,
	}, nil
}

func readBucketPersist(path string, ccFunc ContainerCreateFunc) (*Buckets, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gr.Close()
	var obj Buckets
	if err := gob.NewDecoder(gr).Decode(&obj); err != nil {
		return nil, err
	}
	obj.setCCFunc(ccFunc)
	return &obj, nil
}

func dumpBucketPersist(path string, b Buckets) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gw := gzip.NewWriter(f)
	defer gw.Close()
	return gob.NewEncoder(gw).Encode(b)
}
