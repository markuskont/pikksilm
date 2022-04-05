package enrich

import (
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
)

type Suricata struct {
	Commands *Buckets
	EVE      *Buckets
}

func (s *Suricata) Process(e models.Entry) error {
  return nil
}

func NewSuricata() (*Suricata, error) {
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
	return &Suricata{
		Commands: commands,
	}, nil
}
