package processing

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

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
					c.ClientCorrelated.Expire(context.Background(), e.Key, 1*time.Hour)
				}
			case e, ok := <-c.ChanOnlyNetwork:
				if ok && c.ForwardNetworkEvents && c.ClientOnlyNetwork != nil {
					if err := c.ClientOnlyNetwork.LPush(context.Background(), e.Key, e.Entry).Err(); err != nil {
						lctx.Error(err)
						continue loop
					}
					c.ClientOnlyNetwork.Expire(context.Background(), e.Key, 1*time.Hour)
				}
			}
		}
		return nil
	})
	return nil
}
