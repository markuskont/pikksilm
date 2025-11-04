package processing

import (
	"errors"
	"fmt"
	"time"

	"github.com/elliotchance/orderedmap/v2"
)

type sysmonCache struct {
	data *orderedmap.OrderedMap[string, *SysmonCoreECS]
	size int
}

func (c *sysmonCache) add(a *SysmonCoreECS) (evicted bool) {
	if c.data.Len() >= c.size {
		if oldest := c.data.Front(); oldest != nil {
			evicted = c.data.Delete(oldest.Key)
		}
	}
	c.data.Set(a.Process.EntityID, a)
	return evicted
}

type winlogStats struct {
	count         int
	processed     int
	invalid       int
	sysmon_events struct {
		process_created    int
		network_connection int
		unsuported         int
	}
	cache struct {
		items   int
		evicted int
		hits    int
		misses  int
	}
}

type winlog struct {
	cache    *sysmonCache
	stats    winlogStats
	handlers []HandleWinlog
}

func (w *winlog) Process(event *SysmonCoreECS) error {
	w.stats.count++
	switch event.Winlog.EventID {
	case "1":
		w.stats.sysmon_events.process_created++
		if w.cache.add(event) {
			w.stats.cache.evicted++
		}
		w.stats.cache.items = w.cache.data.Len()
	case "3":
		w.stats.sysmon_events.network_connection++
		val, ok := w.cache.data.Get(event.Process.EntityID)
		if !ok {
			w.stats.cache.misses++
		} else {
			w.stats.cache.hits++
			val.Network = event.Network
			if err := w.handle(val); err != nil {
				return err
			}
		}
	case "":
		w.stats.invalid++
		return nil
	default:
		w.stats.sysmon_events.unsuported++
		return nil
	}
	w.stats.processed++
	return nil
}

func (w winlog) handle(event *SysmonCoreECS) error {
	for _, fn := range w.handlers {
		if err := fn(*event); err != nil {
			return err
		}
	}
	return nil
}

func newWinlog(cache int, handlers []HandleWinlog) (*winlog, error) {
	if cache <= 0 {
		return nil, errors.New("winlog: invalid cache size")
	}
	if len(handlers) == 0 {
		return nil, errors.New("winlog: no result handlers")
	}
	return &winlog{
		cache: &sysmonCache{
			data: orderedmap.NewOrderedMap[string, *SysmonCoreECS](),
			size: cache,
		},
		handlers: handlers,
	}, nil
}

type ConfigProcessWinlog struct {
	ConfigWorkerPool

	RX        <-chan *SysmonCoreECS
	CacheSize int
	Handers   []HandleWinlog
}

func ProcessWinlog(c ConfigProcessWinlog) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("winlog: %s", err)
	}
	if c.RX == nil {
		return errors.New("winlog: missing input")
	}
	c.Pool.Go(func() error {
		log := Logger.With("name", "winlog processor")
		log.Debug("worker start")

		report := time.NewTicker(c.LogInterval)
		defer report.Stop()

		winlog, err := newWinlog(c.CacheSize, c.Handers)
		if err != nil {
			return err
		}
	loop:
		for {
			select {
			case <-report.C:
				log.Info("report", "stats", winlog.stats)
			case <-c.Ctx.Done():
				log.Debug("exit caught")
				break loop
			case event, ok := <-c.RX:
				if !ok {
					log.Debug("channel closed")
					break loop
				}
				if err := winlog.Process(event); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return nil
}
