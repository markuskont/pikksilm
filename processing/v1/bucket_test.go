package processing

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var cfg = BucketsConfig{
	Count: 3,
	Size:  5 * time.Second,
}

func TestBucket(t *testing.T) {
	b, err := newBuckets(bucketsConfig{BucketsConfig: cfg})
	assert.Nil(t, err)

	ts, err := time.Parse(time.RFC3339, "2021-12-04T11:10:45.736Z")
	assert.Nil(t, err)

	b.tryRotate(ts)
	assert.Equal(t, 1, len(b.Buckets))

	b.tryRotate(ts.Add(4 * time.Second))
	assert.Equal(t, 1, len(b.Buckets))

	b.tryRotate(ts.Add(6 * time.Second))
	assert.Equal(t, 2, len(b.Buckets))

	b.tryRotate(ts.Add(12 * time.Second))
	assert.Equal(t, 3, len(b.Buckets))

	b.tryRotate(ts.Add(20 * time.Second))
	assert.Equal(t, 3, len(b.Buckets))
}

func TestInsertCurrent(t *testing.T) {
	b, err := newBuckets(
		bucketsConfig{
			BucketsConfig:       cfg,
			containerCreateFunc: func() any { return make(networkEvents, 0) },
		},
	)
	assert.Nil(t, err)

	b.InsertCurrent(func(b *Bucket) error {
		data, ok := b.Data.(networkEvents)
		assert.True(t, ok)
		data = append(data, networkEntry{
			SrcIP: net.ParseIP("1.2.3.4"),
		})
		data = append(data, networkEntry{
			SrcIP: net.ParseIP("5.6.7.8"),
		})
		b.Data = data
		return nil
	})
	assert.Equal(t, "1.2.3.4", b.Buckets[0].Data.(networkEvents)[0].SrcIP.String())
	assert.Equal(t, "5.6.7.8", b.Buckets[0].Data.(networkEvents)[1].SrcIP.String())
}

func TestBucketExpand(t *testing.T) {
	ts, err := time.Parse(time.RFC3339, "2021-12-04T11:10:45.736Z")
	assert.Nil(t, err)
	buckets, err := newBuckets(
		bucketsConfig{
			BucketsConfig:       cfg,
			containerCreateFunc: func() any { return make(networkEvents, 0) },
		},
	)
	assert.Nil(t, err)

	val1 := networkEvents{
		{SrcIP: net.IPv4(1, 1, 1, 1)},
	}
	_, err = buckets.insert(func(b *Bucket) error {
		b.Data = val1
		return nil
	}, ts)
	assert.Nil(t, err)
	val2 := networkEvents{
		{SrcIP: net.IPv4(1, 1, 2, 1)},
	}
	_, err = buckets.insert(func(b *Bucket) error {
		b.Data = val2
		return nil
	}, ts.Add(6*time.Second))
	assert.Nil(t, err)
	val3 := networkEvents{
		{SrcIP: net.IPv4(1, 1, 3, 1)},
	}
	_, err = buckets.insert(func(b *Bucket) error {
		b.Data = val3
		return nil
	}, ts.Add(12*time.Second))
	assert.Nil(t, err)
	val4 := networkEvents{
		{SrcIP: net.IPv4(1, 1, 4, 1)},
	}
	_, err = buckets.insert(func(b *Bucket) error {
		b.Data = val4
		return nil
	}, ts.Add(18*time.Second))
	assert.Nil(t, err)
	val4expandded := append(val4, networkEntry{SrcIP: net.IPv4(1, 1, 4, 2)})
	_, err = buckets.insert(func(b *Bucket) error {
		b.Data = val4expandded
		return nil
	}, ts.Add(18*time.Second))
	assert.Nil(t, err)

	assert.Equal(t, 3, len(buckets.Buckets))
	assert.Equal(t, val4expandded, buckets.Buckets[2].Data.(networkEvents))
}
