package processing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/markuskont/datamodels"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type SysmonConsumeConfig struct {
	Client  *redis.Client
	Key     string
	Workers int
	Pool    *errgroup.Group
	Ctx     context.Context
	Logger  *logrus.Logger
	Handler MapHandlerFunc
}

func ConsumeSysmonEvents(c SysmonConsumeConfig) error {
	if c.Client == nil {
		return errors.New("sysmon redis client missing")
	}
	if c.Key == "" {
		return errors.New("sysmon redis key missing")
	}
	if c.Workers < 1 {
		return errors.New("invalid sysmon consumer count")
	}
	if c.Pool == nil {
		return errors.New("missing worker pool")
	}
	if c.Ctx == nil {
		return errors.New("missing context")
	}
	if c.Logger == nil {
		return errors.New("missing logger")
	}
	if c.Handler == nil {
		return errors.New("missing data map handler")
	}
	if err := waitOnRedis(c.Ctx, c.Client, c.Logger); err != nil {
		return err
	}

	for i := 0; i < c.Workers; i++ {
		worker := i

		c.Pool.Go(func() error {
			lctx := c.Logger.
				WithField("worker", worker).
				WithField("task", "consume")

			lctx.Info("worker setting up")
		loop:
			for {
				select {
				case <-c.Ctx.Done():
					lctx.Info("caught exit")
					break loop
				default:
					raw, err := c.Client.LPop(context.TODO(), c.Key).Bytes()
					if err != nil {
						if err == redis.Nil {
							time.Sleep(50 * time.Millisecond)
						} else {
							lctx.Error(err)
						}
						continue loop
					}
					var e datamodels.Map
					if err := json.Unmarshal(raw, &e); err != nil {
						lctx.Error(err)
						continue loop
					}
					c.Handler(e)
				}
			}
			return nil
		})
	}
	return nil
}

type SysmonCorrelateConfig struct {
	Workers int
	Pool    *errgroup.Group
	Ctx     context.Context
	Logger  *logrus.Logger
	Shards  *DataMapShards

	LogCorrelations bool
	WinlogConfig
}

func CorrelateSysmonEvents(c SysmonCorrelateConfig) error {
	if c.Workers < 1 {
		return errors.New("invalid worker count")
	}
	if c.Pool == nil {
		return errors.New("missing worker pool")
	}
	if c.Ctx == nil {
		return errors.New("missing context")
	}
	if c.Logger == nil {
		return errors.New("missing logger")
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
					lctx.WithFields(winlog.Stats.fields()).Info("stream report")
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
