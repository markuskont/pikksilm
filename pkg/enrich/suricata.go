package enrich

import (
	"errors"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
)

type SuricataStats struct {
	Total              int
	MissingCommunityID int
	Enriched           int
}

type Suricata struct {
	// cached edr enrichment from Winlog handler
	Commands *Buckets
	// Suricata EVE logs
	EVE *Buckets

	Stats SuricataStats
}

func (s *Suricata) checkEntries(e Entries) error {
	return s.Commands.Check(func(b *Bucket) error {
		edr, ok := b.Data.(CommandEvents)
		if !ok {
			return errors.New("edr cache wrong type on ndr check")
		}
		for _, item := range e {
			if cid, ok := item.GetString("community_id"); !ok {
				// seeing a few of these is most likely stats
				// a lot or all means missing community_id in suricata config
				s.Stats.MissingCommunityID++
			} else {
				if val, seen := edr[cid]; seen {
					item["edr"] = val
					s.Stats.Enriched++
				}
			}
		}
		return nil
	})
}

func (s *Suricata) Process(e models.Entry) error {
	s.Stats.Total++
	b, err := s.EVE.InsertCurrentAndGetVal(func(b *Bucket) error {
		data, ok := b.Data.(Entries)
		if !ok {
			return errors.New("ndr data cache wrong type")
		}
		b.Data = append(data, e)
		return nil
	})
	if err != nil {
		return err
	}
	if b != nil {
		data, ok := b.Data.(Entries)
		if !ok {
			return errors.New("ndr data cache wrong type")
		}
		return s.checkEntries(data)
	}
	return nil
}

type SuricataConfig struct {
	DestQueue string
}

func NewSuricata(c SuricataConfig) (*Suricata, error) {
	if c.DestQueue == "" {
		return nil, errors.New("NDR destination queue missing")
	}
	commands, err := newBuckets(bucketsConfig{
		BucketsConfig: BucketsConfig{
			Count: 2,
			Size:  30 * time.Second,
		},
		ContainerCreateFunc: func() any {
			return make(CommandEvents)
		},
	})
	if err != nil {
		return nil, err
	}
	eve, err := newBuckets(bucketsConfig{
		BucketsConfig: BucketsConfig{
			Count: 2,
			Size:  3 * time.Second,
		},
		ContainerCreateFunc: newEntries,
	})
	if err != nil {
		return nil, err
	}
	return &Suricata{Commands: commands, EVE: eve}, nil
}
