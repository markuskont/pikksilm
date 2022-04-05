package enrich

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/sirupsen/logrus"
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

	enrichmentWriter io.WriteCloser
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
				// FIXME - this counter is wrong due to how bucket checks behave
				// TODO - only increment on first bucket check, others are duplicates
				s.Stats.MissingCommunityID++
			} else {
				if val, seen := edr[cid]; seen {
					item["edr"] = val
					s.Stats.Enriched++
					if err := s.writeEnrichedEntry(item); err != nil {
						logrus.Error(err)
					}
				}
			}
		}
		return nil
	})
}

func (s *Suricata) Process(e models.Entry) (Entries, error) {
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
		return nil, err
	}
	if b != nil {
		data, ok := b.Data.(Entries)
		if !ok {
			return nil, errors.New("ndr data cache wrong type")
		}
		if err := s.checkEntries(data); err != nil {
			return nil, err
		}
		return data, nil
	}
	return nil, nil
}

func (s Suricata) writeEnrichedEntry(e models.Entry) error {
	if writer := s.enrichmentWriter; writer != nil {
		encoded, err := json.Marshal(e)
		if err != nil {
			return err
		}
		_, err = writer.Write(append(encoded, []byte("\n")...))
		return err
	}
	return nil
}

func (s Suricata) Close() error {
	if s.enrichmentWriter != nil {
		return s.enrichmentWriter.Close()
	}
	return nil
}

type SuricataConfig struct {
	// EnrichedJSONPath is optional file path to write out enrichments.
	// Mostly for debugging.
	EnrichedJSONPath string
}

func NewSuricata(c SuricataConfig) (*Suricata, error) {
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
	s := &Suricata{Commands: commands, EVE: eve}
	if c.EnrichedJSONPath != "" {
		f, err := os.Create(c.EnrichedJSONPath)
		if err != nil {
			return nil, err
		}
		s.enrichmentWriter = f
	}
	return s, nil
}
