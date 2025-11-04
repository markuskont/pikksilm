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
}

type suricata struct {
	stats    suricataStats
	cache    *lru.TwoQueueCache[string, SysmonCoreECS]
	bulkSize int
	bulk     []dict.Entry
}

func (s *suricata) store(event dict.Entry) error {
	s.stats.count++
	s.bulk = append(s.bulk, event)
	if len(s.bulk) == s.bulkSize {
		return s.process()
	}
	return nil
}

func (s *suricata) process() error {
	// TODO:
	return nil
}

func newSuricata(cache int, bulk int) (*suricata, error) {
	c, err := lru.New2Q[string, SysmonCoreECS](cache)
	if err != nil {
		return nil, err
	}
	return &suricata{
		cache:    c,
		bulkSize: bulk,
		bulk:     make([]dict.Entry, 0, bulk),
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

		suricata, err := newSuricata(c.Cache, c.BulkSize)
		if err != nil {
			return fmt.Errorf("suricata: %s", err)
		}

	loop:
		for {
			select {
			case <-report.C:
				log.Info("report", "stats", suricata.stats)
			case <-c.Ctx.Done():
				log.Debug("exit caught")
				break loop
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
