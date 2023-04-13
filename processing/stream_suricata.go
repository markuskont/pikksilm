package processing

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/markuskont/datamodels"
)

type SuricataCorrelateConfig struct {
	ConfigStreamWorkers
	Output ConfigStreamRedis

	InputEventShards      *DataMapShards
	CorrelatedEventShards *DataMapShards
}

func CorrelateSuricataEvents(c SuricataCorrelateConfig) error {
	if err := c.ConfigStreamWorkers.Validate(); err != nil {
		return err
	}
	if err := c.Output.Validate(); err != nil {
		return err
	}
	if c.InputEventShards == nil {
		return errors.New("suricata - missing event shards")
	}
	if len(c.InputEventShards.Channels) != c.Workers {
		return errors.New("suricata - worker count does not match event channels")
	}

	if c.CorrelatedEventShards == nil {
		return errors.New("suricata - missing event shards")
	}
	if len(c.InputEventShards.Channels) != c.Workers {
		return errors.New("suricata - worker count does not match event channels")
	}

	if err := waitOnRedis(c.Ctx, c.Output.Client, c.Logger); err != nil {
		return err
	}

	for i := 0; i < c.Workers; i++ {
		worker := i
		c.Pool.Go(func() error {
			lctx := c.Logger.
				WithField("worker", worker).
				WithField("stream", "suricata").
				WithField("queue", c.Output.Key)
			lctx.Info("worker setting up")

			chEvents := c.InputEventShards.Channels[worker]
			if chEvents == nil {
				return errors.New("empty shard - events")
			}

			chCorrelations := c.CorrelatedEventShards.Channels[worker]
			if chCorrelations == nil {
				return errors.New("empty shard - correlations")
			}

			report := time.NewTicker(15 * time.Second)
			defer report.Stop()

			var (
				countEvents           int
				countNoFiveTuple      int
				countNoCID            int
				countErrCommunityID   int
				countErrMarshalJSON   int
				countSuccess          int
				countCorrPickup       int
				countTypeMismatch     int
				countBucketRotates    int
				countSkippedEventType int
			)

			bucketsCorr, err := newBuckets(bucketsConfig{
				BucketsConfig: BucketsConfig{
					Count: 4,
					Size:  300 * time.Second,
				},
				containerCreateFunc: func() any { return make(map[string]*datamodels.SafeMap) },
			})
			if err != nil {
				return err
			}

			bucketsEve, err := newBuckets(bucketsConfig{
				BucketsConfig: BucketsConfig{
					Count: 4,
					Size:  2 * time.Second,
				},
				containerCreateFunc: func() any { return make([]*datamodels.SafeMap, 0) },
			})
			if err != nil {
				return err
			}

			ingoreEventTypes := map[string]bool{
				"flow": true,
				"dns":  true,
			}

		loop:
			for {
				select {
				case corr, ok := <-chCorrelations:
					if !ok {
						break loop
					}
					countCorrPickup++
					bucketsCorr.InsertCurrent(func(b *Bucket) error {
						container, ok := b.Data.(map[string]*datamodels.SafeMap)
						if !ok {
							return errors.New("suricata - invalid bucket data type, expected a map")
						}
						// FIXME - data race here
						cid, ok := corr.GetString("network", "community_id")
						if !ok {
							countNoCID++
							return nil
						}
						container[cid] = corr
						return nil
					})
				case eve, ok := <-chEvents:
					if !ok {
						break loop
					}
					countEvents++
					if eventType, ok := eve.GetString("event_type"); ok && ingoreEventTypes[eventType] {
						encoded, err := json.Marshal(eve.Raw())
						if err != nil {
							countErrMarshalJSON++
							continue loop
						}
						c.Output.Client.LPush(context.TODO(), c.Output.Key, encoded)
						countSkippedEventType++
						continue loop
					}

					val, err := bucketsEve.InsertCurrentAndGetVal(func(b *Bucket) error {
						container, ok := b.Data.([]*datamodels.SafeMap)
						if !ok {
							return errors.New("suricata - invalid bucket data type, expected a map")
						}
						b.Data = append(container, eve)
						return nil
					})
					if err != nil {
						return errors.New("invalid EVE bucket - event insertion")
					} else if val != nil {
						countBucketRotates++
						container, ok := val.Data.([]*datamodels.SafeMap)
						if !ok {
							return errors.New("invalid rotated EVE bucket")
						}
						for _, event := range container {
							id, ok := event.GetString("community_id")
							if ok {
								if err := bucketsCorr.Check(func(b *Bucket) error {
									container, ok := b.Data.(map[string]*datamodels.SafeMap)
									if !ok {
										return errors.New("suricata - invalid bucket data type, expected a map")
									}
									correlation, ok := container[id]
									if ok {
										event.Set(correlation.Raw(), "edr")
										countSuccess++
									}
									return nil
								}); err != nil {
									countTypeMismatch++
								}
							} else {
								countErrCommunityID++
							}

							encoded, err := json.Marshal(event.Raw())
							if err != nil {
								countErrMarshalJSON++
								continue loop
							}
							c.Output.Client.LPush(context.TODO(), c.Output.Key, encoded)
						}
					}

				case <-c.Ctx.Done():
					lctx.Info("caught exit")
					break loop
				case <-report.C:
					lctx.
						WithField("count_events", countEvents).
						WithField("count_no_fivetuple", countNoFiveTuple).
						WithField("count_cid_err", countErrCommunityID).
						WithField("count_no_cid", countNoCID).
						WithField("count_marshal_json_err", countErrMarshalJSON).
						WithField("count_success", countSuccess).
						WithField("count_correlations_pickup", countCorrPickup).
						WithField("count_bucket_err_type_lookup", countTypeMismatch).
						WithField("count_bucket_rotates", countBucketRotates).
						WithField("count_skipped_event_type", countSkippedEventType).
						Info("suricata correlation report")
				}
			}
			return nil
		})
	}
	return nil
}

// SafeCorrelationEventMap is a naive verion of bucketing system for simple lookups
// For now, only needed for doing correlation lookups for Suricata events
type SafeCorrelationEventMap struct {
	sync.RWMutex
	data map[string]datamodels.Map
}

func (s *SafeCorrelationEventMap) Insert(key string, value datamodels.Map) {
	s.Lock()
	defer s.Unlock()
	dst := make(datamodels.Map)
	deepCopyMap(value, dst)
	s.data[key] = dst
}

func (s *SafeCorrelationEventMap) Lookup(key string) (datamodels.Map, bool) {
	s.RLock()
	defer s.RUnlock()
	val, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return val, ok
}

func (s *SafeCorrelationEventMap) Len() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.data)
}

func NewSafeConcurrentMap() *SafeCorrelationEventMap {
	return &SafeCorrelationEventMap{
		RWMutex: sync.RWMutex{},
		data:    make(map[string]datamodels.Map),
	}
}

// go map is a wrapper around pointer types
// sysmon correlator is modifying that map, so passing it into a lookup object
// is pretty much guaranteed to cause race conditions
func deepCopyMap(src map[string]any, dest map[string]any) {
	for key, value := range src {
		switch src[key].(type) {
		case map[string]any:
			dest[key] = map[string]any{}
			deepCopyMap(src[key].(map[string]any), dest[key].(map[string]any))
		default:
			dest[key] = value
		}
	}
}

func getNetworkEntry(eve *datamodels.SafeMap) *networkEntry {
	proto, ok := eve.GetString("proto")
	if !ok {
		return nil
	}
	ipSrc, ok := eve.GetString("src_ip")
	if !ok {
		return nil
	}
	parsedSrcIP := net.ParseIP(ipSrc)
	if parsedSrcIP == nil {
		return nil
	}
	ipDest, ok := eve.GetString("dest_ip")
	if !ok {
		return nil
	}
	parsedDestIP := net.ParseIP(ipDest)
	if parsedDestIP == nil {
		return nil
	}
	portSrc, ok := eve.GetNumber("src_port")
	if !ok {
		return nil
	}
	portDest, ok := eve.GetNumber("dest_port")
	if !ok {
		return nil
	}
	return &networkEntry{
		Proto:    proto,
		SrcIP:    parsedSrcIP,
		DestIP:   parsedDestIP,
		SrcPort:  uint16(portSrc),
		DestPort: uint16(portDest),
	}
}
