package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/markuskont/pikksilm/pkg/enrich"
	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/markuskont/pikksilm/pkg/stream"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the main enrichment procedure",
	Long: `This command enriches network events by correlating sysmon command and network
  events via community ID enrichment.

  pikksilm run`,
	Run: func(cmd *cobra.Command, args []string) {
		defer func() {
			log.Info("good exit")
		}()
		ch := make(chan enrich.Enrichment, 1000)
		ctx, stop := context.WithCancel(context.Background())

		pool := errgroup.Group{}
		// worker to handle system signals
		pool.Go(func() error {
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt)
			<-ch
			stop()
			return nil
		})
		// worker to handle winlogbeat correlation
		pool.Go(func() error {
			defer close(ch)
			c, err := enrich.NewWinlog(enrich.WinlogConfig{
				Buckets: enrich.WinlogBucketsConfig{
					Command: enrich.BucketsConfig{
						Count: viper.GetInt("run.buckets.cmd.count"),
						Size:  viper.GetDuration("run.buckets.cmd.size"),
					},
					Network: enrich.BucketsConfig{
						Count: viper.GetInt("run.buckets.net.count"),
						Size:  viper.GetDuration("run.buckets.net.size"),
					},
				},
				StoreNetEvents: viper.GetBool("run.buckets.net.enable"),
				Destination:    ch,
				WorkDir:        viper.GetString("run.dir.dump"),
			})
			if err != nil {
				return err
			}
			log.
				WithField("cmd_buckets_loaded", c.CmdLen()).
				Info("winlog handler started")

			switch viper.GetString("run.stream.input") {
			case "redis":
				if err := stream.ReadWinlogRedis(ctx, log, c, models.ConfigRedisInstance{
					Host:     viper.GetString("run.stream.winlog.redis.host"),
					Database: viper.GetInt("run.stream.winlog.redis.db"),
					Batch:    100,
					Key:      viper.GetString("run.stream.winlog.redis.key"),
					Password: viper.GetString("run.stream.winlog.redis.password"),
				}); err != nil {
					return err
				}
			case "stdin":
				if err := stream.ReadWinlogStdin(ctx, log, c); err != nil {
					return err
				}
			}
			return c.Close()
		})
		// worker to push correlated items to WISE
		pool.Go(func() error {
			rdb := redis.NewClient(&redis.Options{
				Addr:     viper.GetString("run.stream.wise.redis.host"),
				Password: viper.GetString("run.stream.wise.redis.password"),
				DB:       viper.GetInt("run.stream.wise.redis.db"),
			})
			if resp := rdb.Ping(context.TODO()); resp == nil {
				return fmt.Errorf(
					"Unable to ping redis at %s",
					viper.GetString("run.stream.wise.redis.host"),
				)
			}
		loop:
			for item := range ch {
				encoded, err := models.Decoder.Marshal(item.Entry)
				if err != nil {
					log.Error(err)
					continue loop
				}
				if err := rdb.LPush(context.Background(), item.Key, encoded).Err(); err != nil {
					log.Error(err)
					continue loop
				}
			}
			return nil
		})
		if err := pool.Wait(); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	pFlags := runCmd.PersistentFlags()

	pFlags.String("dir-dump", "", "Directory to store persistence")
	viper.BindPFlag("run.dir.dump", pFlags.Lookup("dir-dump"))

	pFlags.Int("buckets-cmd-count", 3, "Number of command event cache buckets")
	viper.BindPFlag("run.buckets.cmd.count", pFlags.Lookup("buckets-cmd-count"))

	pFlags.Duration("buckets-cmd-size", 5*time.Minute, "Command event bucket size")
	viper.BindPFlag("run.buckets.cmd.size", pFlags.Lookup("buckets-cmd-size"))

	pFlags.Int("buckets-net-count", 6, "Number of network event cache buckets")
	viper.BindPFlag("run.buckets.net.count", pFlags.Lookup("buckets-net-count"))

	pFlags.Duration("buckets-net-size", 5*time.Second, "Network event bucket size")
	viper.BindPFlag("run.buckets.net.size", pFlags.Lookup("buckets-net-size"))

	pFlags.Bool("buckets-net-enable", true, "Enable network bucket collection. "+
		"Good for out of order events. ")
	viper.BindPFlag("run.buckets.net.enable", pFlags.Lookup("buckets-net-enable"))

	pFlags.String("stream-input", "redis", "Redis, stdin")
	viper.BindPFlag("run.stream.input", pFlags.Lookup("stream-input"))

	pFlags.Bool("stream-suricata", false, "Enable Suricata enrichment")
	viper.BindPFlag("run.stream.suricata", pFlags.Lookup("stream-suricata"))

	initLogStreamSectionRedis(pFlags, "winlog", 0, "winlogbeat")
	initLogStreamSectionRedis(pFlags, "wise", 1, "NA")

	initLogStreamSectionRedis(pFlags, "suricata_alert_source", 0, "eve_alert")
	initLogStreamSectionRedis(pFlags, "suricata_eve_source", 0, "eve")

	initLogStreamSectionRedis(pFlags, "suricata_alert_dest", 0, "eve_alert_enriched")
	initLogStreamSectionRedis(pFlags, "suricata_eve_dest", 0, "eve_enriched")
}

func initLogStreamSectionRedis(
	pFlags *pflag.FlagSet,
	section string,
	db int,
	key string,
) {
	pFlags.String(section+"-redis-host", "localhost:6379", "Host to redis instance.")
	viper.BindPFlag("run.stream."+section+".redis.host", pFlags.Lookup(section+"-redis-host"))

	pFlags.String(section+"-redis-password", "", "Redis password.")
	viper.BindPFlag("run.stream."+section+".redis.password", pFlags.Lookup(section+"-redis-password"))

	pFlags.Int(section+"-redis-db", db, "Redis database.")
	viper.BindPFlag("run.stream."+section+".redis.db", pFlags.Lookup(section+"-redis-db"))

	pFlags.String(section+"-redis-key", key, "Redis queue key")
	viper.BindPFlag("run.stream."+section+".redis.key", pFlags.Lookup(section+"-redis-key"))
}
