package processing

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

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

type ConfigStreamWorkers struct {
	Name    string
	Workers int
	Pool    *errgroup.Group
	Ctx     context.Context
	Logger  *logrus.Logger
}

func (c ConfigStreamWorkers) Validate() error {
	if c.Workers < 1 {
		return errors.New("invalid consumer count")
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
	return nil
}

func (c *ConfigStreamWorkers) SetNoWorkers() ConfigStreamWorkers {
	if c.Logger != nil {
		c.
			Logger.
			WithField("name", c.Name).
			Debug("Resetting worker count to 1")
	}
	c.Workers = 1
	return *c
}

type ConfigStreamRedis struct {
	Client *redis.Client
	Key    string
}

func (c ConfigStreamRedis) Validate() error {
	if c.Client == nil {
		return errors.New("sysmon redis client missing")
	}
	if c.Key == "" {
		return errors.New("sysmon redis key missing")
	}

	return nil
}
