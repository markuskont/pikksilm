package processing

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/markuskont/datamodels"
	"github.com/sirupsen/logrus"
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

// unlike persist.go functions, this is really specific to correlation worker
func correlateWriteDump(
	c SysmonCorrelateConfig,
	lctx *logrus.Entry,
	worker int,
	data []Bucket,
) {
	if c.WorkDir != "" {
		path := correlateDumpFmt(c.WorkDir, worker)
		lctx.WithField("path", path).Debug("writing command peristence")
		if err := dumpPersist(path, data); err != nil {
			lctx.Error(err)
		}
	}
}
