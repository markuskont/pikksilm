package processing

import (
	"errors"
	"fmt"
	"os"
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

func (w winlog) dump() []SysmonCoreECS {
	tx := make([]SysmonCoreECS, 0, w.cache.data.Len())
	for el := w.cache.data.Front(); el != nil; el = el.Next() {
		tx = append(tx, *el.Value)
	}
	return tx
}

func newWinlog(cache int, handlers []HandleWinlog, preload []SysmonCoreECS) (*winlog, error) {
	if cache <= 0 {
		return nil, errors.New("winlog: invalid cache size")
	}
	if len(handlers) == 0 {
		return nil, errors.New("winlog: no result handlers")
	}
	w := &winlog{
		cache: &sysmonCache{
			data: orderedmap.NewOrderedMap[string, *SysmonCoreECS](),
			size: cache,
		},
		handlers: handlers,
	}
	for _, v := range preload {
		w.cache.add(&v)
	}
	return w, nil
}

type ConfigProcessWinlog struct {
	ConfigWorkerPool

	RX        <-chan *SysmonCoreECS
	CacheSize int
	Handers   []HandleWinlog

	Persist struct {
		Preload []SysmonCoreECS
		Handler func([]SysmonCoreECS) error
	}
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

		winlog, err := newWinlog(c.CacheSize, c.Handers, c.Persist.Preload)
		if err != nil {
			return err
		}

		persist := c.Persist.Handler
		if persist == nil {
			persist = func(sce []SysmonCoreECS) error {
				log.Warn("persistence disabled")
				return nil
			}
		}
	loop:
		for {
			select {
			case <-report.C:
				winlog.stats.cache.items = winlog.cache.data.Len()
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
		return persist(winlog.dump())
	})
	return nil
}

func LoadPersist(path string, dst any) (bool, error) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&dst); err != nil {
		return false, err
	}
	return true, nil
}
