package enrich

import (
	"errors"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/satta/gommunityid"
)

type CommandItem struct {
	Data       models.Entry
	LastAccess time.Time
}

type CommandCache map[string]*CommandItem

func (c CommandCache) Check(id string) (models.Entry, bool) {
	val, ok := c[id]
	if !ok {
		return nil, false
	}
	val.LastAccess = time.Now()
	return val.Data, ok
}

type SimpleCache map[string]models.Entry

// Correlate is handler for enrichment
type Correlate struct {
	CommandCache
	gommunityid.CommunityID

	// Community ID lookup table of positive correlations
	// TODO - make this a sharded pass to workers
	Enrichments map[string]models.Entry

	// Store network events that don't have a cmd yet
	ConnectionCache *Buckets

	window time.Duration
}

func (c *Correlate) Winlog(e models.Entry) error {
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

		// we expect potentially a lot of network events per one command
		command, ok := c.CommandCache.Check(entityID)
		if !ok {
			return c.ConnectionCache.Insert(func(b *Bucket) error {
				b.NetworkEvents = append(b.NetworkEvents, *ne)
				return nil
			})
		}

		// FIXME - refactor repeat code
		id, err := ne.CommunityID(c.CommunityID)
		if err != nil {
			return err
		}
		c.Enrichments[id] = command
	case "1":
		// command event
		// we expect only one command event per entity id
		c.CommandCache[entityID] = &CommandItem{Data: e}
		// now we should also do a lookup to see if any network events came before the command
		c.ConnectionCache.Check(func(b *Bucket) error {
			if len(b.NetworkEvents) == 0 {
				return nil
			}
			for _, ne := range b.NetworkEvents {
				if ne.GUID == entityID {
					// FIXME - refactor repeat code
					id, err := ne.CommunityID(c.CommunityID)
					if err != nil {
						return err
					}
					c.Enrichments[id] = e
				}
			}
			return nil
		}, c.window)
	default:
		return nil
	}
	return nil
}

type CorrelateConfig struct {
	BucketCount  int
	BucketSize   time.Duration
	LookupWindow time.Duration
}

func NewCorrelate(c CorrelateConfig) (*Correlate, error) {
	cid, err := gommunityid.GetCommunityIDByVersion(1, 0)
	if err != nil {
		return nil, err
	}
	connCache, err := NewBuckets(c.BucketCount, c.BucketSize)
	if err != nil {
		return nil, err
	}
	if c.LookupWindow == 0 {
		return nil, errors.New("Lookup window missing")
	}
	return &Correlate{
		CommandCache:    make(CommandCache),
		CommunityID:     cid,
		Enrichments:     make(map[string]models.Entry),
		ConnectionCache: connCache,
    window: c.LookupWindow,
	}, nil
}
