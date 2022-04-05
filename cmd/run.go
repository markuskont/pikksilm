package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
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

			switch viper.GetString("run.stream.edr.input") {
			case "redis":
				if err := stream.ReadWinlogRedis(ctx, log, c, models.ConfigRedisInstance{
					Host:     viper.GetString("run.stream.edr.redis.host"),
					Password: viper.GetString("run.stream.edr.redis.password"),
					Database: viper.GetInt("run.stream.edr.redis.db"),
					Key:      viper.GetString("run.stream.edr.redis.queue.input"),
					Batch:    10,
				}); err != nil {
					return err
				}
			case "stdin":
				if err := stream.ReadWinlogStdin(ctx, log, c); err != nil {
					return err
				}
			default:
				return fmt.Errorf("invalid EDR input %s", viper.GetString("run.stream.edr.input"))
			}
			return c.Close()
		})
		var (
			wiseCh chan enrich.Enrichment
			suriCh chan enrich.Enrichment
		)
		if viper.GetBool("run.stream.ndr.enabled") {
			log.Info("NDR enrichment enabled")
			wiseCh = make(chan enrich.Enrichment, 100)
			suriCh = make(chan enrich.Enrichment, 100)
			// worker to split enrichmetns between WISE and Suricata
			pool.Go(func() error {
				defer close(wiseCh)
				defer close(suriCh)
				for item := range ch {
					wiseCh <- item
					suriCh <- item
				}
				return nil
			})
			// worker to work on suricata events
			pool.Go(func() error {
				c := &redis.Options{
					Addr:     viper.GetString("run.stream.ndr.redis.host"),
					DB:       viper.GetInt("run.stream.ndr.redis.db"),
					Password: viper.GetString("run.stream.ndr.redis.password"),
				}
				log.WithFields(map[string]any{
					"addr": c.Addr,
					"db":   c.DB,
				}).Debug("NDR connecting to redis")
				rdb := redis.NewClient(c)
				if resp := rdb.Ping(context.TODO()); resp == nil {
					return fmt.Errorf("Unable to ping redis at %s", c.Addr)
				}
				pipeline := rdb.Pipeline()
				defer pipeline.Close()

				sessions, err := enrich.NewSuricata(enrich.SuricataConfig{
					DestQueue: viper.GetString("run.stream.ndr.redis.queue.output.sessions"),
				})
				if err != nil {
					return err
				}
				alerts, err := enrich.NewSuricata(enrich.SuricataConfig{
					DestQueue: viper.GetString("run.stream.ndr.redis.queue.output.alerts"),
				})
				if err != nil {
					return err
				}
				tick := time.NewTicker(3 * time.Second)
				defer tick.Stop()

				var (
					countEnrichPickups int
				)
			loop:
				for {
					select {
					case <-tick.C:
						log.
							WithField("enrichment_pickup", countEnrichPickups).
							WithField("ndr_sessions", sessions.Stats.Total).
							WithField("ndr_alerts", alerts.Stats.Total).
							WithField("ndr_enrichments", sessions.Stats.Enriched+alerts.Stats.Enriched).
							WithField(
								"cid_missing",
								sessions.Stats.MissingCommunityID+alerts.Stats.MissingCommunityID,
							).
							Info("NDR report")
					case enrichment, ok := <-suriCh:
						if !ok {
							break loop
						}
						sessions.Commands.InsertCurrent(func(b *enrich.Bucket) error {
							data, ok := b.Data.(enrich.CommandEvents)
							if !ok {
								return errors.New("suricata handler cmd event insert wrong type")
							}
							data[enrichment.Key] = enrichment.Entry
							return nil
						})
						countEnrichPickups++
					default:
						if err := stream.RedisBatchProcess(pipeline, sessions, viper.GetString(
							"run.stream.ndr.redis.queue.input.sessions",
						), 10); err != nil {
							log.Error(err)
						}
						if err := stream.RedisBatchProcess(pipeline, alerts, viper.GetString(
							"run.stream.ndr.redis.queue.input.alerts",
						), 10); err != nil {
							log.Error(err)
						}
					}
				}
				return nil
			})
		} else {
			log.Warn("NDR enrichment not enabled")
			// no need for split if suricata is not enabled
			wiseCh = ch
		}
		// worker to push correlated items to WISE
		pool.Go(func() error {
			c := &redis.Options{
				Addr:     viper.GetString("run.stream.wise.redis.host"),
				Password: viper.GetString("run.stream.wise.redis.password"),
				DB:       viper.GetInt("run.stream.wise.redis.db"),
			}
			rdb := redis.NewClient(c)
			if resp := rdb.Ping(context.TODO()); resp == nil {
				return fmt.Errorf("Unable to ping redis at %s", c.Addr)
			}
		loop:
			for item := range wiseCh {
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

	pFlags.String("stream-edr-input", "redis", "Redis, stdin")
	viper.BindPFlag("run.stream.edr.input", pFlags.Lookup("stream-edr-input"))

	addConfigRedisHost(pFlags, "edr", "EDR (sysmon) input stream", 0)
	addConfigRedisHost(pFlags, "ndr", "NDR (suricata) duplex stream", 0)
	addConfigRedisHost(pFlags, "wise", "TI (Arkime WISE) output stream", 1)

	pFlags.Bool("stream-ndr-enabled", false, "Enable NDR (Suricata) enrichment")
	viper.BindPFlag("run.stream.ndr.enabled", pFlags.Lookup("stream-ndr-enabled"))

	addConfigRedisQueue(pFlags, "edr", "input", "winlogbeat", "EDR events input")

	addConfigRedisQueue(pFlags, "ndr", "input.alerts", "alerts", "NRD alerts input")
	addConfigRedisQueue(pFlags, "ndr", "input.sessions", "sessions", "NRD sessions input")

	addConfigRedisQueue(pFlags, "ndr", "output.alerts", "alerts_edr", "NRD alerts input")
	addConfigRedisQueue(pFlags, "ndr", "output.sessions", "sessions_edr", "NRD sessions input")
}

func addConfigRedisHost(pFlags *pflag.FlagSet, section, description string, db int) {
	pFlags.String("stream-"+section+"-redis-host", "localhost:6379", "Redis host for "+description)
	viper.BindPFlag("run.stream."+section+".redis.host", pFlags.Lookup("stream-"+section+"-redis-host"))

	pFlags.String("stream-"+section+"-redis-password", "", "Password for "+description+". Empty means no auth.")
	viper.BindPFlag("run.stream."+section+".redis.password", pFlags.Lookup("stream-"+section+"-redis-password"))

	pFlags.Int("stream-"+section+"-redis-db", db, "Redis database for "+description)
	viper.BindPFlag("run.stream."+section+".redis.db", pFlags.Lookup("stream-"+section+"-redis-db"))
}

func addConfigRedisQueue(pFlags *pflag.FlagSet, section, name, fallback, description string) {
	flag := fmt.Sprintf("stream-%s-redis-queue-%s", section, strings.ReplaceAll(name, ".", "-"))
	pFlags.String(flag, fallback, "Redis queue for "+description)
	viper.BindPFlag("run.stream."+section+".redis.queue."+name, pFlags.Lookup(flag))
}
