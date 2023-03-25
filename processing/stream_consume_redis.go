package processing

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/markuskont/datamodels"
)

type ConfigConsumeRedis struct {
	ConfigStreamWorkers
	ConfigStreamRedis

	Handler MapHandlerFunc
}

func ConsumeRedis(c ConfigConsumeRedis) error {
	if err := c.ConfigStreamWorkers.Validate(); err != nil {
		return err
	}
	if err := c.ConfigStreamRedis.Validate(); err != nil {
		return err
	}

	if c.Handler == nil {
		return errors.New("missing data map handler")
	}
	if err := waitOnRedis(c.Ctx, c.Client, c.Logger); err != nil {
		return err
	}

	// TODO: a single redis consumer that uses pipelines is much better
	// still need load-balancing for JSON decode though
	// we should make one worker that consumes messages and sends
	// to decode workers via channel or shard assignment
	for i := 0; i < c.Workers; i++ {
		worker := i

		c.Pool.Go(func() error {
			lctx := c.Logger.
				WithField("worker", worker).
				WithField("queue", c.Key)

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
