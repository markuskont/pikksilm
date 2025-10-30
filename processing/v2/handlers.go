package processing

import (
	"context"
	"io"
	"os"
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

type HandleWinlog func(SysmonCoreECS) error

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
