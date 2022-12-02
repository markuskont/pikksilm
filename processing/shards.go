package processing

import (
	"context"
	"errors"

	"github.com/markuskont/datamodels"
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
