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

func ReadWinlogStdin(
	ctx context.Context,
	log *logrus.Logger,
	w *enrich.Winlog,
) error {
	scanner := bufio.NewScanner(os.Stdin)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
loop:
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			break loop
		case <-tick.C:
			log.
				WithFields(w.Stats.Fields()).
				Info("EDR report")
		default:
			var e models.Entry
			if err := models.Decoder.Unmarshal(scanner.Bytes(), &e); err != nil {
				log.Error(err)
				continue loop
			}
			_, err := w.Process(e)
			if err != nil {
				log.Error(err)
			}
		}
	}
	return scanner.Err()
}

func ReadWinlogRedis(
	ctx context.Context,
	log *logrus.Logger,
	w *enrich.Winlog,
	c models.ConfigRedisInstance,
) error {
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
	} else if err := resp.Err(); err != nil {
		return err
	}
	pipeline := rdb.Pipeline()
	defer pipeline.Close()

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()

outer:
	for {
		select {
		case <-tick.C:
			log.
				WithFields(w.Stats.Fields()).
				Info("EDR report")
		case <-ctx.Done():
			break outer
		default:
			if err := RedisBatchProcess(pipeline, w, c.Key, "", c.Batch, log); err != nil {
				log.Error(err)
			}
		}
	}
	return nil
}

func RedisBatchProcess(
	pipeline redis.Pipeliner,
	p enrich.Processor,
	src, dest string,
	batch int64,
	log *logrus.Logger,
) error {
	data := pipeline.LRange(context.TODO(), src, 0, batch)
	pipeline.LTrim(context.TODO(), src, batch, -1)
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
		bulk, err := p.Process(e)
		if err != nil {
			log.Error(err)
			continue loop
		}
		if bulk != nil && dest != "" {
			if err := RedisPushEntries(pipeline, bulk, dest); err != nil {
				log.Error(err)
				continue loop
			}
		}
	}
	return err
}

func RedisPushEntries(pipeline redis.Pipeliner, b enrich.Entries, key string) error {
	for _, item := range b {
		encoded, err := models.Decoder.Marshal(item)
		if err != nil {
			return err
		}
		pipeline.RPush(context.Background(), key, encoded)
	}
	_, err := pipeline.Exec(context.Background())
	return err
}
