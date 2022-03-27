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

type WinlogStats struct {
	// general statistics
	Enriched        int
	Sent            int
	Dropped         int
	NetEventsStored int
	NetEventsPopped int
	CmdBucketMoves  int
	Count           int
	SeenGUID        map[string]bool
}

func (ws WinlogStats) Fields() map[string]any {
	return map[string]any{
		"enriched":          ws.Enriched,
		"emitted":           ws.Sent,
		"dropped":           ws.Dropped,
		"count":             ws.Count,
		"enriched_percent":  float64(ws.Enriched) / float64(ws.Count),
		"net_events_stored": ws.NetEventsStored,
		"net_events_popped": ws.NetEventsPopped,
		"cmd_bucket_moves":  ws.CmdBucketMoves,
	}
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
	// weather to keep network events in buckets or not
	// for potential out of order messages, is memory intentsive
	storeNetEvents bool

	Stats WinlogStats
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
	c.Stats.Count++
	switch eventID {
	case "3":
		// network event
		ne, err := models.ExtractNetworkEntry(e, entityID)
		if err != nil {
			return err
		}

		// check if we already have corresponding command event cached
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
			// command was already found in latest bucket, no need to move it
			if c.Buckets.current.Equal(b.Time) {
				return nil
			}
			// move command to latest bucket
      // we might observe a heap of network events for single command (like beacons)
			// don't want command to rotate while network activity is still active
			delete(data.CommandEvents, entityID)
			return c.Buckets.InsertCurrent(func(b *Bucket) error {
				data, ok := b.Data.(*WinlogData)
				if !ok {
					return errors.New("invalid bucket data type")
				}
				data.CommandEvents[entityID] = command
				c.Stats.CmdBucketMoves++
				return nil
			})
		}, c.window)

		// TODO - this actually seems kinda pointless, maybe ditch this code path entirely
		// if no corresponding command found, cache for out of order lookup
		if !found && c.storeNetEvents {
			c.Stats.NetEventsStored++
			return c.Buckets.InsertCurrent(func(b *Bucket) error {
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
		c.Buckets.InsertCurrent(func(b *Bucket) error {
			data, ok := b.Data.(*WinlogData)
			if !ok {
				return errors.New("invalid bucket data type")
			}
			data.CommandEvents[entityID] = e
			return nil
		})
		// TODO - this actually seems kinda pointless, maybe ditch this code path entirely
		if c.storeNetEvents {
			// now we should also do a lookup to see if any network events came before the command
			return c.Buckets.Check(func(b *Bucket) error {
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
						c.Stats.NetEventsPopped++
						c.send(e, id)
					}
				}
				return nil
			}, c.window)
		}
	default:
		return nil
	}
	return nil
}

func (c *Winlog) send(e models.Entry, key string) {
	c.Stats.Enriched++
	select {
	case c.enrichments <- Enrichment{
		Entry: e,
		Key:   key,
	}:
		c.Stats.Sent++
	default:
		c.Stats.Dropped++
	}
}

type WinlogConfig struct {
	BucketCount    int
	BucketSize     time.Duration
	LookupWindow   time.Duration
	StoreNetEvents bool
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
		CommunityID:    cid,
		Buckets:        connCache,
		window:         c.LookupWindow,
		storeNetEvents: c.StoreNetEvents,
		enrichments:    make(chan Enrichment),
	}, nil
}
