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
	Enrichments map[string]models.Entry
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
			// command not yet seen
			// might never be seen, might be out of order
			// FIXME - should cache this event for period T and recheck later
			// TODO - store item in slice / linked list and periodically clean it up by
			// checking each entry against command cache and then dropping entire bucket
			return nil
		}

		id, err := ne.CommunityID(c.CommunityID)
    if err != nil {
      return err
    }
		c.Enrichments[id] = command
	case "1":
		// command event
		// we expect only one command event per entity id
		c.CommandCache[entityID] = &CommandItem{Data: e}
	default:
		return nil
	}
	return nil
}

func NewCorrelate() (*Correlate, error) {
	cid, err := gommunityid.GetCommunityIDByVersion(1, 0)
	if err != nil {
		return nil, err
	}
	return &Correlate{
		CommandCache: make(CommandCache),
		CommunityID:  cid,
		Enrichments:  make(map[string]models.Entry),
	}, nil
}
