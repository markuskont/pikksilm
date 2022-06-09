package processing

import (
	"errors"
	"io"

	"github.com/markuskont/datamodels"
	"github.com/satta/gommunityid"
)

type winlogStats struct {
	// general statistics
	Enriched        int
	Dropped         int
	NetEventsStored int
	NetEventsPopped int
	CmdBucketMoves  int
	Count           int
	CountCommand    int
	CountNetwork    int
	InvalidEvent    int
	MissingGUID     int
	MissingEventID  int
	SeenGUID        map[string]bool
}

func (ws winlogStats) fields() map[string]any {
	return map[string]any{
		"enriched":          ws.Enriched,
		"dropped":           ws.Dropped,
		"count":             ws.Count,
		"count_command":     ws.CountCommand,
		"count_network":     ws.CountNetwork,
		"net_events_stored": ws.NetEventsStored,
		"net_events_popped": ws.NetEventsPopped,
		"cmd_bucket_moves":  ws.CmdBucketMoves,
		"guid_missing":      ws.MissingGUID,
		"invalid_event":     ws.InvalidEvent,
	}
}

type winlogBuckets struct {
	network  *buckets
	commands *buckets
}

// Winlog is handler for enrichment
type Winlog struct {
	gommunityid.CommunityID

	chCorrelated  chan EncodedEntry
	chOnlyNetwork chan EncodedEntry

	writerCorrelate io.WriteCloser

	buckets *winlogBuckets

	// weather to keep network events in buckets or not
	// for potential out of order messages, is memory intentsive
	storeNetEvents bool

	// emit network events that do not have positive correlation
	forwardNetEvents bool

	Stats winlogStats
}

func (c *Winlog) Close() error {
	if c.writerCorrelate != nil {
		c.writerCorrelate.Close()
	}
	return nil
}

func (c Winlog) CmdLen() int { return len(c.buckets.commands.Buckets) }

func (c *Winlog) Process(e datamodels.Map) error {
	entityID, ok := e.GetString("process", "entity_id")
	if !ok {
		c.Stats.MissingGUID++
		return nil
	}
	eventID, ok := e.GetString("winlog", "event_id")
	if !ok {
		c.Stats.MissingEventID++
		return ErrInvalidEvent{
			Key: "winlog.event_id",
			Raw: e,
		}
	}
	c.Stats.Count++
	switch eventID {
	case "3":
		c.Stats.CountNetwork++
		// network event
		ne, err := extractNetworkEntryECS(e, entityID)
		if err != nil {
			return err
		}
		id, err := ne.communityID(c.CommunityID)
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
			command.Set(id, "network", "community_id")
			c.sendCorrelated(command, id)
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
			return err
		}

		// Event 1 caching was not found, forward network event as-is if enabled
		if !found && c.forwardNetEvents {
			e.Set(id, "network", "community_id")
			c.sendNetworkEvent(e, id)
		}

		// TODO - this actually seems kinda pointless, maybe ditch this code path entirely
		// if no corresponding command found, cache for out of order lookup
		if !found && c.storeNetEvents {
			c.Stats.NetEventsStored++
			err := c.buckets.network.InsertCurrent(func(b *Bucket) error {
				data, ok := b.Data.(networkEvents)
				if !ok {
					return errors.New("invalid bucket data type")
				}
				b.Data = append(data, *ne)
				return nil
			})
			return err
		}

	case "1":
		c.Stats.CountCommand++
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
				data, ok := b.Data.(networkEvents)
				if !ok {
					return errors.New("invalid bucket data type")
				}
				if len(data) == 0 {
					return nil
				}
				for _, ne := range data {
					if ne.GUID == entityID {
						id, err := ne.communityID(c.CommunityID)
						if err != nil {
							return err
						}
						c.Stats.NetEventsPopped++
						e.Set(id, "network", "community_id")
						// TODO - handle potential error
						c.sendCorrelated(e, id)
					}
				}
				return nil
			})
			return err
		}
	default:
		return nil
	}
	return nil
}

func (c *Winlog) sendCorrelated(e datamodels.Map, key string) error {
	c.Stats.Enriched++
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	c.chCorrelated <- EncodedEntry{Entry: data, Key: key}
	if c.writerCorrelate != nil {
		if _, err := c.writerCorrelate.Write(data); err != nil {
			return err
		}
	}
	return nil
}

func (c *Winlog) sendNetworkEvent(e datamodels.Map, key string) error {
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	c.chOnlyNetwork <- EncodedEntry{Entry: data, Key: key}
	return nil
}

type WinlogBucketsConfig struct {
	Command BucketsConfig
	Network BucketsConfig
}

type WinlogConfig struct {
	Commands             buckets
	Buckets              WinlogBucketsConfig
	StoreNetEvents       bool
	WorkDir              string
	ChanCorrelated       chan EncodedEntry
	ChanOnlyNetwork      chan EncodedEntry
	ForwardNetworkEvents bool
}

func newWinlog(c WinlogConfig, cmdPersist []Bucket, corrWriter io.WriteCloser) (*Winlog, error) {
	if c.ChanCorrelated == nil {
		return nil, errors.New("missing correlation dest chan")
	}
	cid, err := gommunityid.GetCommunityIDByVersion(1, 0)
	if err != nil {
		return nil, err
	}
	w := &Winlog{
		CommunityID:     cid,
		storeNetEvents:  c.StoreNetEvents,
		writerCorrelate: corrWriter,
	}

	commands, err := newBuckets(bucketsConfig{
		BucketsConfig:       c.Buckets.Command,
		containerCreateFunc: func() any { return make(CommandEvents) },
		data:                cmdPersist,
	})
	if err != nil {
		return nil, err
	}
	network, err := newBuckets(bucketsConfig{
		BucketsConfig:       c.Buckets.Network,
		containerCreateFunc: func() any { return make(networkEvents, 0) },
		data:                nil,
	})
	if err != nil {
		return nil, err
	}
	w.buckets = &winlogBuckets{
		network:  network,
		commands: commands,
	}

	if c.ChanCorrelated != nil {
		w.chCorrelated = c.ChanCorrelated
	} else {
		w.chCorrelated = make(chan EncodedEntry)
	}
	if c.ChanOnlyNetwork != nil {
		w.chOnlyNetwork = c.ChanOnlyNetwork
	} else {
		w.chOnlyNetwork = make(chan EncodedEntry)
	}
	w.forwardNetEvents = c.ForwardNetworkEvents
	return w, nil
}
