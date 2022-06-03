package processing

import (
	"context"
	"errors"
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

			winlog, err := NewWinlog(c.WinlogConfig)
			if err != nil {
				return err
			}

			report := time.NewTicker(15 * time.Second)
			defer report.Stop()
		loop:
			for {
				select {
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

type WiseConfig struct {
	Ctx    context.Context
	Logger *logrus.Logger
	Pool   *errgroup.Group

	ClientCorrelated  *redis.Client
	ClientOnlyNetwork *redis.Client

	ForwardNetworkEvents bool

	ChanCorrelated  <-chan EncodedEntry
	ChanOnlyNetwork <-chan EncodedEntry
}

func OutputWISE(c WiseConfig) error {
	if c.ClientCorrelated == nil {
		return errors.New("WISE redis host missing for correlations")
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
	if c.ChanCorrelated == nil {
		return errors.New("enrichment channel missing")
	}
	if c.ChanOnlyNetwork == nil {
		return errors.New("network event channel missing")
	}
	if c.ForwardNetworkEvents {
		if c.ClientOnlyNetwork == nil {
			return errors.New("WISE redis host missing for network events")
		}
		if err := waitOnRedis(c.Ctx, c.ClientOnlyNetwork, c.Logger); err != nil {
			return err
		}
	}
	if err := waitOnRedis(c.Ctx, c.ClientCorrelated, c.Logger); err != nil {
		return err
	}
	c.Pool.Go(func() error {
		lctx := c.Logger.
			WithField("worker", "correlate").
			WithField("task", "wise")
		lctx.Info("worker setting up")
	loop:
		for {
			select {
			case <-c.Ctx.Done():
				lctx.Info("caught exit")
				break loop
			case e, ok := <-c.ChanCorrelated:
				if ok {
					if err := c.ClientCorrelated.LPush(context.Background(), e.Key, e.Entry).Err(); err != nil {
						lctx.Error(err)
						continue loop
					}
					c.ClientCorrelated.Expire(context.Background(), e.Key, 5*time.Minute)
				}
			case e, ok := <-c.ChanOnlyNetwork:
				if ok && c.ForwardNetworkEvents && c.ClientOnlyNetwork != nil {
					if err := c.ClientOnlyNetwork.LPush(context.Background(), e.Key, e.Entry).Err(); err != nil {
						lctx.Error(err)
						continue loop
					}
					c.ClientOnlyNetwork.Expire(context.Background(), e.Key, 5*time.Minute)
				}
			}
		}
		return nil
	})
	return nil
}

func waitOnRedis(ctx context.Context, client *redis.Client, logger *logrus.Logger) error {
	// wait until redis is up
	if err := client.Ping(context.TODO()).Err(); err != nil {
		logger.WithField("err", err).Error("could not contact redis, will retry")
	loop:
		for {
			select {
			case <-ctx.Done():
				return errors.New("could not contact redis")
			default:
				time.Sleep(1 * time.Second)
				if err := client.Ping(context.TODO()).Err(); err == nil {
					break loop
				}
			}
		}
	}
	logger.Info("redis connection established")
	return nil
}

func balanceString(value string, count uint64) int {
	return assignWorker(hash(value), count)
}

func assignWorker(hash, workerCount uint64) int { return int(hash % workerCount) }
