package processing

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
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
