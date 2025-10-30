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

// FIXME: this needs to be configurable
var Logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
	Level: slog.LevelDebug,
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
