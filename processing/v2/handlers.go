package processing

import (
	"context"
	"io"
	"os"

	"github.com/StamusNetworks/goupil/dict"
	"github.com/go-redis/redis/v8"
)

var newline = []byte("\n")

type HandleConsume func(context.Context, []byte) error

type HandleSysmonCoreBlocking chan *SysmonCoreECS

func (h HandleSysmonCoreBlocking) Func() HandleConsume {
	return func(ctx context.Context, b []byte) error {
		var obj SysmonCoreECS
		if err := json.Unmarshal(b, &obj); err != nil {
			return err
		}
		// check global waitgroup context to avoid deadlocking SIGINT on blocking send
		select {
		case h <- &obj:
		case <-ctx.Done():
			return nil
		}
		return nil
	}
}

type HandleDecodeGeneric chan dict.Entry

func (h HandleDecodeGeneric) Func() HandleConsume {
	return func(ctx context.Context, b []byte) error {
		var obj dict.Entry
		if err := json.Unmarshal(b, &obj); err != nil {
			return err
		}
		select {
		case h <- obj:
		case <-ctx.Done():
			return nil
		}
		return nil
	}
}

type HandleWinlog func(SysmonCoreECS) error

type HandleBridge struct {
	ch  chan SysmonCoreECS
	ctx context.Context
}

func (h HandleBridge) Func() HandleWinlog {
	return func(sce SysmonCoreECS) error {
		select {
		case h.ch <- sce:
		case <-h.ctx.Done():
			return nil
		}
		return nil
	}
}

func (h *HandleBridge) Close() error {
	close(h.ch)
	return nil
}

func (h HandleBridge) RX() <-chan SysmonCoreECS { return h.ch }

func NewHandleBridge(ctx context.Context, buffer int) *HandleBridge {
	return &HandleBridge{
		ch:  make(chan SysmonCoreECS, buffer),
		ctx: ctx,
	}
}

type HandleOutputIO struct {
	io.WriteCloser
	written int
}

func (h *HandleOutputIO) Func() HandleWinlog {
	return func(sce SysmonCoreECS) error {
		encoded, err := json.Marshal(sce)
		if err != nil {
			return err
		}
		n, err := h.Write(append(encoded, newline...))
		h.written += n
		return err
	}
}

func (h *HandleOutputIO) Close() error {
	return h.WriteCloser.Close()
}

func NewWriterFile(path string) (*HandleOutputIO, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &HandleOutputIO{WriteCloser: f}, nil
}

type HandleOutputWise struct {
	redis *redis.Client
}

func (h HandleOutputWise) Func() HandleWinlog {
	return func(sce SysmonCoreECS) error {
		encoded, err := json.Marshal(sce)
		if err != nil {
			return err
		}
		return h.
			redis.
			LPush(context.Background(), sce.Network.CommunityID, encoded).
			Err()
	}
}

func NewWriterWISE(c ConfigRedis) (*HandleOutputWise, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &HandleOutputWise{
		redis: redis.NewClient(&redis.Options{
			Addr:     c.Addr,
			DB:       c.DB,
			Password: c.Password,
		})}, nil
}
