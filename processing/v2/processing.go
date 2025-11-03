package processing

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"
	"golang.org/x/sync/errgroup"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

var LogLevel = new(slog.LevelVar)

var Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	Level: LogLevel,
}))

type ConfigWorkerPool struct {
	Pool        *errgroup.Group
	Ctx         context.Context
	LogInterval time.Duration
}

func (c ConfigWorkerPool) Validate() error {
	if c.Pool == nil {
		return errors.New("missing worker pool")
	}
	if c.Ctx == nil {
		return errors.New("missing context")
	}
	if c.LogInterval == 0 {
		return errors.New("missing report interval")
	}
	return nil
}

type ConfigRedis struct {
	// some producers like WISE use dynamic keys, so Key will be redundant
	// but we still want to validate in other cases, so dynkey skips that check
	DynKey   bool
	Key      string
	Addr     string
	DB       int
	Password string
}

func (c ConfigRedis) Validate() error {
	if c.Addr == "" {
		return errors.New("missing redis address")
	}
	if !c.DynKey && c.Key == "" {
		return errors.New("missing redis key")
	}
	return nil
}
