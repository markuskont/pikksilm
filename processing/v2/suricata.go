package processing

import (
	"errors"
	"fmt"
	"time"

	"github.com/StamusNetworks/goupil/dict"
	lru "github.com/hashicorp/golang-lru/v2"
)

type suricataStats struct {
	count  int
	cached int
	bulk   struct {
		oversize  int
		rotations int
	}
	cache struct {
		hits   int
		misses int
	}
}

type suricataEnrich struct {
	stats    suricataStats
	cache    *lru.TwoQueueCache[string, SysmonCoreECS]
	bulkSize int
	// TODO: two layer system where we have two bulks and we rotate the second, then replace with first
	// current approach has could have issue where last messages are not delayed long enough
	bulk     []dict.Entry
	handlers []HandleEncoded
}

func (s *suricataEnrich) store(event dict.Entry) error {
	s.stats.count++
	s.bulk = append(s.bulk, event)
	if len(s.bulk) == s.bulkSize {
		s.stats.bulk.oversize++
		return s.process()
	}
	return nil
}

func (s *suricataEnrich) process() error {
	for _, event := range s.bulk {
		if communityID, ok := event.GetString("community_id"); ok {
			if correlation, correlated := s.cache.Get(communityID); correlated {
				event.Set(correlation, "edr")
				s.stats.cache.hits++
			} else {
				s.stats.cache.misses++
			}
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return err
		}
		for _, handle := range s.handlers {
			if err := handle(encoded); err != nil {
				return err
			}
		}
	}
	s.bulk = make([]dict.Entry, 0, s.bulkSize)
	s.stats.bulk.rotations++
	return nil
}

func newSuricata(cache int, bulk int, handlers []HandleEncoded) (*suricataEnrich, error) {
	if len(handlers) == 0 {
		return nil, errors.New("missing handlers")
	}
	c, err := lru.New2Q[string, SysmonCoreECS](cache)
	if err != nil {
		return nil, err
	}
	return &suricataEnrich{
		cache:    c,
		bulkSize: bulk,
		bulk:     make([]dict.Entry, 0, bulk),
		handlers: handlers,
	}, nil
}

type ConfigProcessSuricata struct {
	ConfigWorkerPool

	Cache    int
	BulkSize int
	Delay    time.Duration

	RX struct {
		Events       <-chan dict.Entry
		Correlations <-chan SysmonCoreECS
	}

	Handlers []HandleEncoded
}

func ProcessSuricata(c ConfigProcessSuricata) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.RX.Events == nil {
		return errors.New("suricata: missing input - events")
	}
	if c.RX.Correlations == nil {
		return errors.New("suricata: missing input - correlations")
	}
	if c.Cache <= 0 {
		return errors.New("suricata: invalid cache")
	}
	if c.BulkSize == 0 {
		return errors.New("suricata: maximum bulk size undefined")
	}
	if c.Delay == 0 {
		return errors.New("suricata: processing delay undefined")
	}
	c.Pool.Go(func() error {
		log := Logger.With("name", "suricata processor")
		log.Debug("worker start")

		report := time.NewTicker(c.LogInterval)
		defer report.Stop()

		suricata, err := newSuricata(c.Cache, c.BulkSize, c.Handlers)
		if err != nil {
			return fmt.Errorf("suricata: %s", err)
		}

		process := time.NewTicker(c.Delay)
		defer process.Stop()

	loop:
		for {
			select {
			case <-report.C:
				log.Info("report", "stats", suricata.stats)
			case <-c.Ctx.Done():
				log.Debug("exit caught")
				break loop
			case <-process.C:
				if err := suricata.process(); err != nil {
					return err
				}
			case event, ok := <-c.RX.Events:
				if !ok {
					log.Debug("channel closed")
					break loop
				}
				if err := suricata.store(event); err != nil {
					return err
				}
			case corr, ok := <-c.RX.Correlations:
				if !ok {
					log.Debug("channel closed")
					break loop
				}
				suricata.cache.Add(corr.Network.CommunityID, corr)
				suricata.stats.cached = suricata.cache.Len()
			}
		}
		return nil
	})
	return nil
}
