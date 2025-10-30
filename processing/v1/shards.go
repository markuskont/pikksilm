package processing

import (
	"context"
	"errors"

	"github.com/markuskont/datamodels"
)

type MapHandlerFunc func(*datamodels.SafeMap)

type DataMapShards struct {
	Name            string
	Channels        []chan *datamodels.SafeMap
	Ctx             context.Context
	Len             uint64
	CountMissingKey uint64
}

func (s *DataMapShards) Handler(balanceKey ...string) (MapHandlerFunc, error) {
	if len(balanceKey) == 0 {
		return nil, errors.New("shard balancer handler missing balance key")
	}
	return func(m *datamodels.SafeMap) {
		entityID, ok := m.GetString(balanceKey...)
		if !ok {
			return
		}
		select {
		case s.Channels[balanceString(entityID, s.Len)] <- m:
		case <-s.Ctx.Done():
			return
		}
	}, nil
}

func NewDataMapShards(ctx context.Context, workers int, name string) (*DataMapShards, error) {
	if workers < 1 {
		return nil, errors.New("invalid worker count for shard init")
	}
	shards := make([]chan *datamodels.SafeMap, workers)
	for i := range shards {
		ch := make(chan *datamodels.SafeMap)
		shards[i] = ch
	}
	return &DataMapShards{
		Name:     name,
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
