package enrich

import (
	"errors"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/satta/gommunityid"
)

type Enrichment struct {
	Key   string
	Entry models.Entry
}

// Winlog is handler for enrichment
type Winlog struct {
	gommunityid.CommunityID

	// Enrichments map[string]models.Entry
	enrichments chan Enrichment

	// Store network events that don't have a cmd yet
	Buckets *Buckets

	// time window for lookups
	window time.Duration

	// general statistics
	Enriched int
	Sent     int
	Dropped  int
}

func (c *Winlog) Enrichments() <-chan Enrichment {
	return c.enrichments
}

func (c *Winlog) Process(e models.Entry) error {
	entityID, ok := e.GetString("process", "entity_id")
	if !ok {
		return errors.New("entity id missing")
	}
	eventID, ok := e.GetString("winlog", "event_id")
	if !ok {
		return errors.New("event id missing")
	}
	// current time is used as basis for all correlation ops
	// now := time.Now()
	switch eventID {
	case "3":
		// network event
		ne, err := models.ExtractNetworkEntry(e, entityID)
		if err != nil {
			return err
		}

		var found bool
		c.Buckets.Check(func(b *Bucket) error {
			data, ok := b.Data.(*WinlogData)
			if !ok {
				return errors.New("invalid bucket data type")
			}
			command, ok := data.CommandEvents[entityID]
			if !ok {
				return nil
			}
			id, err := ne.CommunityID(c.CommunityID)
			if err != nil {
				return err
			}
			c.send(command, id)
			found = true
			return nil
		}, c.window)

		if !found {
			return c.Buckets.Insert(func(b *Bucket) error {
				data, ok := b.Data.(*WinlogData)
				if !ok {
					return errors.New("invalid bucket data type")
				}
				data.NetworkEvents = append(data.NetworkEvents, *ne)
				return nil
			})
		}
	case "1":
		// command event
		// we expect only one command event per entity id
		c.Buckets.Insert(func(b *Bucket) error {
			data, ok := b.Data.(*WinlogData)
			if !ok {
				return errors.New("invalid bucket data type")
			}
			data.CommandEvents[entityID] = e
			return nil
		})
		// now we should also do a lookup to see if any network events came before the command
		c.Buckets.Check(func(b *Bucket) error {
			data, ok := b.Data.(*WinlogData)
			if !ok {
				return errors.New("invalid bucket data type")
			}
			if len(data.NetworkEvents) == 0 {
				return nil
			}
			for _, ne := range data.NetworkEvents {
				if ne.GUID == entityID {
					id, err := ne.CommunityID(c.CommunityID)
					if err != nil {
						return err
					}
					c.send(e, id)
				}
			}
			return nil
		}, c.window)
	default:
		return nil
	}
	return nil
}

func (c *Winlog) send(e models.Entry, key string) {
	c.Enriched++
	select {
	case c.enrichments <- Enrichment{
		Entry: e,
		Key:   key,
	}:
		c.Sent++
	default:
		c.Dropped++
	}
}

type WinlogConfig struct {
	BucketCount  int
	BucketSize   time.Duration
	LookupWindow time.Duration
}

func NewWinlog(c WinlogConfig) (*Winlog, error) {
	cid, err := gommunityid.GetCommunityIDByVersion(1, 0)
	if err != nil {
		return nil, err
	}
	connCache, err := newBuckets(bucketsConfig{
		Count: c.BucketCount,
		Size:  c.BucketSize,
		ContainerCreateFunc: func() any {
			return &WinlogData{
				NetworkEvents: make([]models.NetworkEntry, 0),
				CommandEvents: make(map[string]models.Entry),
			}
		},
	})
	if err != nil {
		return nil, err
	}
	if c.LookupWindow == 0 {
		return nil, errors.New("Lookup window missing")
	}
	return &Winlog{
		CommunityID: cid,
		Buckets:     connCache,
		window:      c.LookupWindow,
		enrichments: make(chan Enrichment),
	}, nil
}
