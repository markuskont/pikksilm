package enrich

import (
	"errors"

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

	buckets *winlogBuckets

	// weather to keep network events in buckets or not
	// for potential out of order messages, is memory intentsive
	storeNetEvents bool

	Stats WinlogStats
}

func (c *Winlog) Enrichments() <-chan Enrichment {
	return c.enrichments
}

func (c *Winlog) Process(e models.Entry) error {
	provider, ok := e.GetString("event", "provider")
	if !ok {
		return errors.New("event provider missing")
	}
	if provider != "Microsoft-Windows-Sysmon" {
		return nil
	}
	eventID, ok := e.GetString("winlog", "event_id")
	if !ok {
		return errors.New("event id missing")
	}
	c.Stats.Count++
	switch eventID {
	case "3":
		entityID, ok := e.GetString("winlog", "event_data", "ProcessGuid")
		if !ok {
			entityID, ok = e.GetString("winlog", "event_data", "SourceProcessGUID")
			if !ok {
				return errors.New("entity id missing")
			}
		}
		// network event
		ne, err := models.ExtractNetworkEntry(e, entityID)
		if err != nil {
			return err
		}

		// check if we already have corresponding command event cached
		var found bool
		if err := c.buckets.commands.Check(func(b *Bucket) error {
			data, ok := b.Data.(CommandEvents)
			if !ok {
				return errors.New("invalid bucket data type")
			}
			command, ok := data[entityID]
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
			if c.buckets.commands.current.Equal(b.Time) {
				return nil
			}
			// move command to latest bucket
			// we might observe a heap of network events for single command (like beacons)
			// don't want command to rotate while network activity is still active
			delete(data, entityID)
			return c.buckets.commands.InsertCurrent(func(b *Bucket) error {
				data, ok := b.Data.(CommandEvents)
				if !ok {
					return errors.New("invalid bucket data type")
				}
				data[entityID] = command
				c.Stats.CmdBucketMoves++
				return nil
			})
		}); err != nil {
			return err
		}

		// TODO - this actually seems kinda pointless, maybe ditch this code path entirely
		// if no corresponding command found, cache for out of order lookup
		if !found && c.storeNetEvents {
			c.Stats.NetEventsStored++
			return c.buckets.network.InsertCurrent(func(b *Bucket) error {
				data, ok := b.Data.(NetworkEvents)
				if !ok {
					return errors.New("invalid bucket data type")
				}
				b.Data = append(data, *ne)
				return nil
			})
		}

	case "1":
		entityID, ok := e.GetString("winlog", "event_data", "ProcessGuid")
		if !ok {
			return errors.New("entity id missing")
		}
		// command event
		// we expect only one command event per entity id
		c.buckets.commands.InsertCurrent(func(b *Bucket) error {
			data, ok := b.Data.(CommandEvents)
			if !ok {
				return errors.New("invalid bucket data type")
			}
			data[entityID] = e
			return nil
		})
		// TODO - this actually seems kinda pointless, maybe ditch this code path entirely
		if c.storeNetEvents {
			// now we should also do a lookup to see if any network events came before the command
			return c.buckets.network.Check(func(b *Bucket) error {
				data, ok := b.Data.(NetworkEvents)
				if !ok {
					return errors.New("invalid bucket data type")
				}
				if len(data) == 0 {
					return nil
				}
				for _, ne := range data {
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
			})
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

type WinlogBucketsConfig struct {
	Command BucketsConfig
	Network BucketsConfig
}

type WinlogConfig struct {
	Buckets        WinlogBucketsConfig
	StoreNetEvents bool
}

func NewWinlog(c WinlogConfig) (*Winlog, error) {
	cid, err := gommunityid.GetCommunityIDByVersion(1, 0)
	if err != nil {
		return nil, err
	}
	commands, err := newBuckets(bucketsConfig{
		BucketsConfig:       c.Buckets.Command,
		ContainerCreateFunc: func() any { return make(CommandEvents) },
	})
	if err != nil {
		return nil, err
	}
	network, err := newBuckets(bucketsConfig{
		BucketsConfig:       c.Buckets.Network,
		ContainerCreateFunc: func() any { return make(NetworkEvents, 0) },
	})
	if err != nil {
		return nil, err
	}
	return &Winlog{
		CommunityID: cid,
		enrichments: make(chan Enrichment),
		buckets: &winlogBuckets{
			commands: commands,
			network:  network,
		},
		storeNetEvents: c.StoreNetEvents,
	}, nil
}
