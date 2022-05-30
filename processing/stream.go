package processing

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/markuskont/datamodels"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type MapHandlerFunc func(datamodels.Map)

type DataMapShards struct {
	Channels []chan datamodels.Map
	Ctx      context.Context
	Len      uint64
}

func (s *DataMapShards) Handler() MapHandlerFunc {
	return func(m datamodels.Map) {
		entityID, ok := m.GetString("process", "entity_id")
		if ok {
			select {
			case s.Channels[balanceString(entityID, s.Len)] <- m:
			case <-s.Ctx.Done():
				return
			}
		}
	}
}

func NewDataMapShards(ctx context.Context, workers int) (*DataMapShards, error) {
	if workers < 1 {
		return nil, errors.New("invalid worker count for shard init")
	}
	shards := make([]chan datamodels.Map, workers)
	for i := range shards {
		ch := make(chan datamodels.Map)
		shards[i] = ch
	}
	return &DataMapShards{
		Ctx:      ctx,
		Channels: shards,
		Len:      uint64(len(shards)),
	}, nil
}

func (s *DataMapShards) Close() error {
	for i := range s.Channels {
		close(s.Channels[i])
	}
	return nil
}

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
	// wait until redis is up
	if err := c.Client.Ping(context.TODO()).Err(); err != nil {
		c.Logger.WithField("err", err).Logger.Error("could not contact redis, will retry")
	loop:
		for {
			select {
			case <-c.Ctx.Done():
				return errors.New("could not contact redis")
			default:
				time.Sleep(1 * time.Second)
				if err := c.Client.Ping(context.TODO()).Err(); err == nil {
					break loop
				}
			}
		}
	}
	c.Logger.Info("redis connection established")

	var wg sync.WaitGroup
	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		worker := i

		c.Pool.Go(func() error {
			defer wg.Done()
			lctx := c.Logger.
				WithField("count", worker).
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
							time.Sleep(50 * time.Microsecond)
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
	for i := 0; i < c.Workers; i++ {
		worker := i
		ch := c.Shards.Channels[worker]
		if ch == nil {
			return errors.New("empty shard")
		}
		c.Pool.Go(func() error {
			lctx := c.Logger.
				WithField("count", worker).
				WithField("task", "correlate")
			lctx.Info("worker setting up")
		loop:
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						break loop
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

func balanceString(value string, count uint64) int {
	return assignWorker(hash(value), count)
}

func assignWorker(hash, workerCount uint64) int { return int(hash % workerCount) }
