package processing

import (
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/markuskont/datamodels"
)

type BucketHandlerFunc func(*Bucket) error
type ContainerCreateFunc func() any

type Entries []datamodels.Map
type NetworkEvents []NetworkEntry
type CommandEvents map[string]datamodels.Map

func newEntries() any { return make(Entries, 0) }

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

	// execute this function whenever new bucket is created
	// for initializing data container
	onCreateFunc ContainerCreateFunc
}

func (b *Buckets) insert(fn BucketHandlerFunc, ts time.Time) (*Bucket, error) {
	rotated, err := b.tryRotate(ts)
	if err != nil {
		return rotated, err
	}
	return rotated, fn(&b.Buckets[len(b.Buckets)-1])
}

func (b *Buckets) InsertCurrent(fn BucketHandlerFunc) error {
	_, err := b.InsertCurrentAndGetVal(fn)
	return err
}

// InsertCurrentAndRetrieve wraps around InsertCurrent but returns dropped
// bucket in case there was successful rotation. If nothing got rotated, then
// first return value is nil.
func (b *Buckets) InsertCurrentAndGetVal(fn BucketHandlerFunc) (*Bucket, error) {
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

func (b *Buckets) tryRotate(ts time.Time) (*Bucket, error) {
	// append new bucket in case period is exceeded
	if b.Current.IsZero() || b.rollover(ts) {
		b.Current = ts.Truncate(b.Size)
		b.First = b.Current
		b.Buckets = append(b.Buckets, *NewBucket(b.Current, b.onCreateFunc))
	}
	// truncate bucket set in case current count is above max
	if current := len(b.Buckets); current > 1 && current > b.Count {
		rotated := b.Buckets[0]
		b.Buckets = b.Buckets[1:]
		return &rotated, nil
	}
	return nil, nil
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
			return readCmdBucketData(c.Persist, c.ContainerCreateFunc)
		}
	}
	return &Buckets{
		Buckets:      make([]Bucket, 0, c.Count),
		Size:         c.Size,
		Count:        c.Count,
		onCreateFunc: c.ContainerCreateFunc,
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
	if err := json.NewDecoder(gr).Decode(&obj); err != nil {
		return nil, err
	}
	ptr := &obj
	ptr.onCreateFunc = ccFunc
	return &obj, nil
}

// decoded Buckets will be of type any, items need to be recast properly
func castConcreteCmdEvents(buckets []Bucket) {
	if len(buckets) == 0 {
		return
	}
	for i, val := range buckets {
		concrete := make(CommandEvents)
		if cmd, ok := val.Data.(map[string]any); ok {
			for k, v := range cmd {
				if entry, ok := v.(map[string]any); ok {
					concrete[k] = datamodels.Map(entry)
				}
			}
		}
		buckets[i].Data = concrete
	}
}

func readCmdBucketData(path string, ccFunc ContainerCreateFunc) (*Buckets, error) {
	b, err := readBucketPersist(path, ccFunc)
	if err != nil {
		return nil, err
	}
	castConcreteCmdEvents(b.Buckets)
	return b, nil
}

func dumpBucketPersist(path string, b Buckets) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	gw := gzip.NewWriter(f)
	defer gw.Close()
	return json.NewEncoder(gw).Encode(b)
}
