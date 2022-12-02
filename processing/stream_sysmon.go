package processing

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"
)

type SysmonCorrelateConfig struct {
	ConfigStreamWorkers

	Shards          *DataMapShards
	LogCorrelations bool
	WinlogConfig
}

func CorrelateSysmonEvents(c SysmonCorrelateConfig) error {
	if err := c.Validate(); err != nil {
		return err
	}
	if c.Shards == nil {
		return errors.New("missing shard config")
	}
	if len(c.Shards.Channels) != c.Workers {
		return errors.New("worker count does not match channels")
	}
	var persist bool
	if dir := c.WinlogConfig.WorkDir; dir != "" {
		persist = true
		pth := path.Join(dir, "correlate.json")
		if !dumpNotExists(pth) {
			f, err := os.Open(pth)
			if err != nil {
				return err
			}
			var meta metaCorrelate
			if err := json.NewDecoder(f).Decode(&meta); err != nil {
				return err
			}
			if meta.Workers != c.Workers {
				c.
					Logger.
					WithField("old", meta.Workers).
					WithField("new", c.Workers).
					Warn("correlate worker count does not match old one, disabling persist")
				persist = false
			}
			f.Close()
		} else {
			c.Logger.
				WithField("path", pth).
				Warning("correlate meta dump does not exist")
		}
		f, err := os.Create(pth)
		if err != nil {
			return err
		}
		if err := json.NewEncoder(f).Encode(metaCorrelate{Workers: c.Workers}); err != nil {
			return err
		}
		f.Close()
	}
	for i := 0; i < c.Workers; i++ {
		worker := i
		ch := c.Shards.Channels[worker]
		if ch == nil {
			return errors.New("empty shard")
		}
		c.Pool.Go(func() error {
			lctx := c.Logger.
				WithField("worker", worker).
				WithField("task", "correlate")
			lctx.Info("worker setting up")

			var writer io.WriteCloser
			if c.LogCorrelations {
				if c.WorkDir == "" {
					return errors.New("correlation logging requires a working directory")
				}
				fp := path.Join(c.WorkDir, fmt.Sprintf("worker-%d-correlations.json", worker))
				lctx.Info("setting up correlation logger")
				f, err := os.OpenFile(fp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0777)
				if err != nil {
					return err
				}
				writer = f
			}

			var data []Bucket
			if persist && c.WorkDir != "" {
				if pth := correlateDumpFmt(c.WorkDir, worker) + ".gz"; !dumpNotExists(pth) {
					if err := loadPersist(pth, &data); err != nil {
						lctx.Error(err)
					}
					lctx.WithField("items", len(data)).Debug("loaded command persistence")
				} else {
					lctx.WithField("path", pth).Warn("no command persistence to load")
				}
			}

			winlog, err := newWinlog(c.WinlogConfig, data, writer)
			if err != nil {
				return err
			}
			defer winlog.Close()

			report := time.NewTicker(15 * time.Second)
			defer report.Stop()

			persist := time.NewTicker(1 * time.Hour)
			defer persist.Stop()

			defer correlateWriteDump(c, lctx, worker, winlog.buckets.commands.Buckets)
		loop:
			for {
				select {
				case <-persist.C:
					// periodic command dump
					correlateWriteDump(c, lctx, worker, winlog.buckets.commands.Buckets)
				case <-report.C:
					lctx.WithFields(winlog.Stats.fields()).Info("sysmpn correlation report")
				case entry, ok := <-ch:
					if !ok {
						break loop
					}
					if err := winlog.Process(entry); err != nil {
						lctx.Error(err)
					}
				case <-c.Ctx.Done():
					lctx.Info("caught exit")
					break loop
				}
			}
			return nil
		})
	}
	return nil
}
