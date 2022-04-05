package enrich

import (
	"errors"
	"path"

	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/satta/gommunityid"
)

type WinlogStats struct {
	// general statistics
	Enriched        int
	Sent            int
	Dropped         int
	NetEventsStored int
	NetEventsPopped int
	CmdBucketMoves  int
	Count           int
	CountCommand    int
	CountNetwork    int
	SeenGUID        map[string]bool
}

func (ws WinlogStats) Fields() map[string]any {
	return map[string]any{
		"enriched":          ws.Enriched,
		"emitted":           ws.Sent,
		"dropped":           ws.Dropped,
		"count":             ws.Count,
		"count_command":     ws.CountCommand,
		"count_network":     ws.CountNetwork,
		"enriched_percent":  float64(ws.Enriched) / float64(ws.CountNetwork),
		"net_events_stored": ws.NetEventsStored,
		"net_events_popped": ws.NetEventsPopped,
		"cmd_bucket_moves":  ws.CmdBucketMoves,
	}
}

type winlogBuckets struct {
	network  *Buckets
	commands *Buckets
}

// Winlog is handler for enrichment
type Winlog struct {
	gommunityid.CommunityID

	// Enrichments map[string]models.Entry
	enrichments chan Enrichment

	buckets *winlogBuckets

	persistCommand string

	// weather to keep network events in buckets or not
	// for potential out of order messages, is memory intentsive
	storeNetEvents bool

	Stats WinlogStats
}

func (c *Winlog) Close() error {
	if c.persistCommand != "" {
		if err := dumpBucketPersist(c.persistCommand, *c.buckets.commands); err != nil {
			return err
		}
	}
	return nil
}

func (c *Winlog) Enrichments() <-chan Enrichment {
	return c.enrichments
}

func (c Winlog) CmdLen() int { return len(c.buckets.commands.Buckets) }

func (c *Winlog) Process(e models.Entry) (Entries, error) {
	entityID, ok := e.GetString("process", "entity_id")
	if !ok {
		return nil, errors.New("entity id missing")
	}
	eventID, ok := e.GetString("winlog", "event_id")
	if !ok {
		return nil, errors.New("event id missing")
	}
	c.Stats.Count++
	switch eventID {
	case "3":
		// network event
		ne, err := models.ExtractNetworkEntry(e, entityID)
		if err != nil {
			return nil, err
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
			command.Set(id, "network", "community_id")
			c.send(command, id)
			found = true
			// command was already found in latest bucket, no need to move it
			if c.buckets.commands.Current.Equal(b.Time) {
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
			return nil, err
		}

		// TODO - this actually seems kinda pointless, maybe ditch this code path entirely
		// if no corresponding command found, cache for out of order lookup
		if !found && c.storeNetEvents {
			c.Stats.NetEventsStored++
			err := c.buckets.network.InsertCurrent(func(b *Bucket) error {
				data, ok := b.Data.(NetworkEvents)
				if !ok {
					return errors.New("invalid bucket data type")
				}
				b.Data = append(data, *ne)
				return nil
			})
			return nil, err
		}

	case "1":
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
			err := c.buckets.network.Check(func(b *Bucket) error {
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
						e.Set(id, "network", "community_id")
						c.send(e, id)
					}
				}
				return nil
			})
			return nil, err
		}
	default:
		return nil, nil
	}
	return nil, nil
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
	WorkDir        string
	Destination    chan Enrichment
}

func NewWinlog(c WinlogConfig) (*Winlog, error) {
	cid, err := gommunityid.GetCommunityIDByVersion(1, 0)
	if err != nil {
		return nil, err
	}
	w := &Winlog{
		CommunityID:    cid,
		storeNetEvents: c.StoreNetEvents,
	}

	if c.WorkDir != "" {
		w.persistCommand = path.Join(c.WorkDir, "event_id_1.json.gz")
	}

	commands, err := newBuckets(bucketsConfig{
		BucketsConfig:       c.Buckets.Command,
		ContainerCreateFunc: func() any { return make(CommandEvents) },
		Persist:             w.persistCommand,
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
	w.buckets = &winlogBuckets{
		network:  network,
		commands: commands,
	}

	if c.Destination != nil {
		w.enrichments = c.Destination
	} else {
		w.enrichments = make(chan Enrichment)
	}
	return w, nil
}
