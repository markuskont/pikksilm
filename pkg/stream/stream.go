package stream

import (
	"bufio"
	"os"
	"time"

	"github.com/markuskont/pikksilm/pkg/enrich"
	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/sirupsen/logrus"
)

func ReadWinlogStdin(log *logrus.Logger, c *enrich.Winlog) error {
	scanner := bufio.NewScanner(os.Stdin)
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
loop:
	for scanner.Scan() {
		select {
		case <-tick.C:
			log.
				WithFields(c.Stats.Fields()).
				Info("enrichment report")
		default:
		}
		var e models.Entry
		if err := models.Decoder.Unmarshal(scanner.Bytes(), &e); err != nil {
			log.Error(err)
			continue loop
		}
		if err := c.Process(e); err != nil {
			log.Error(err)
		}
	}
	return scanner.Err()
}
