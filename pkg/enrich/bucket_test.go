package enrich

import (
	"net"
	"testing"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/stretchr/testify/assert"
)

func TestBucket(t *testing.T) {
	b, err := NewBuckets(3, 5*time.Second)
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

func TestInsert(t *testing.T) {
	b, err := NewBuckets(3, 5*time.Second)
	assert.Nil(t, err)

	b.Insert(func(b *Bucket) error {
		b.NetworkEvents = append(b.NetworkEvents, models.NetworkEntry{
			SrcIP: net.ParseIP("1.2.3.4"),
		})
		b.NetworkEvents = append(b.NetworkEvents, models.NetworkEntry{
			SrcIP: net.ParseIP("5.6.7.8"),
		})
		return nil
	})
	assert.Equal(t, "1.2.3.4", b.Buckets[0].NetworkEvents[0].SrcIP.String())
	assert.Equal(t, "5.6.7.8", b.Buckets[0].NetworkEvents[1].SrcIP.String())
}
