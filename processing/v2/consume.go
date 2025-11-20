package processing

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type ConfigConsume struct {
	ConfigWorkerPool

	Redis ConfigRedis
	TX    HandleConsume
}

func Consume(c ConfigConsume) error {
	if err := c.Validate(); err != nil {
		return fmt.Errorf("redis: %s", err)
	}
	if err := c.Redis.Validate(); err != nil {
		return fmt.Errorf("redis: %s", err)
	}
	if c.TX == nil {
		return errors.New("redis: missing consume handler")
	}

	client := redis.NewClient(&redis.Options{
		Addr:     c.Redis.Addr,
		DB:       c.Redis.DB,
		Password: c.Redis.Password,
	})

	c.Pool.Go(func() error {
		var (
			count  int
			errors int
		)

		log := Logger.With(
			"key", c.Redis.Key,
			"name", "redis consumer",
		)
		log.Debug("worker start")
		defer log.Info("Done")

		report := time.NewTicker(c.LogInterval)
		defer report.Stop()
	loop:
		for {
			select {
			case <-report.C:
				log.Info("report",
					"count", count,
					"errors", errors,
				)
			case <-c.Ctx.Done():
				log.Debug("exit caught")
				break loop
			default:
				results, err := client.LPopCount(context.TODO(), c.Redis.Key, 1000).Result()
				if err != nil {
					if err == redis.Nil {
						time.Sleep(50 * time.Millisecond)
					} else {
						log.Error(err.Error())
					}
					continue loop
				}
				for _, result := range results {
					if err := c.TX(c.Ctx, []byte(result)); err != nil {
						errors++
						continue loop
					}
					count++
				}
			}
		}
		return nil
	})
	return nil
}
