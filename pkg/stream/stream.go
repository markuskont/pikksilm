package stream

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/markuskont/pikksilm/pkg/enrich"
	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/sirupsen/logrus"
)

func ReadWinlogStdin(log *logrus.Logger, w *enrich.Winlog) error {
	scanner := bufio.NewScanner(os.Stdin)
	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()
loop:
	for scanner.Scan() {
		select {
		case <-tick.C:
			log.
				WithFields(w.Stats.Fields()).
				Info("enrichment report")
		default:
		}
		var e models.Entry
		if err := models.Decoder.Unmarshal(scanner.Bytes(), &e); err != nil {
			log.Error(err)
			continue loop
		}
		if err := w.Process(e); err != nil {
			log.Error(err)
		}
	}
	return scanner.Err()
}

func ReadWinlogRedis(log *logrus.Logger, w *enrich.Winlog, c models.ConfigRedisInstance) error {
	if c.Batch == 0 {
		return errors.New("winlog redis batch size not configured")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     c.Host,
		DB:       c.Database,
		Password: c.Password,
	})
	if resp := rdb.Ping(context.TODO()); resp == nil {
		return fmt.Errorf("Unable to ping redis at %s", c.Host)
	}
	pipeline := rdb.Pipeline()
	defer pipeline.Close()

	tick := time.NewTicker(3 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			log.
				WithFields(w.Stats.Fields()).
				Info("enrichment report")
		default:
		}
		data := pipeline.LRange(context.TODO(), c.Key, 0, c.Batch)
		pipeline.LTrim(context.TODO(), c.Key, c.Batch, -1)
		_, err := pipeline.Exec(context.TODO())
		if err != nil {
			return err
		}
		result, err := data.Result()
		if err != nil {
			return err
		}
		if len(result) == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	loop:
		for _, item := range result {
			var e models.Entry
			if err := models.Decoder.Unmarshal([]byte(item), &e); err != nil {
				log.Error(err)
				continue loop
			}
			if err := w.Process(e); err != nil {
				log.Error(err)
			}
		}
	}
	return nil
}
