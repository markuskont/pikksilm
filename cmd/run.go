package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/markuskont/pikksilm/processing"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

func run(cmd *cobra.Command, args []string) {
	defer func() {
		log.Info("good exit")
	}()

	ctx, stop := context.WithCancel(context.Background())
	pool, poolCtx := errgroup.WithContext(ctx)
	pool.Go(func() error {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		select {
		case <-ch:
		case <-poolCtx.Done():
		}
		stop()
		return nil
	})

	shardsSysmon, err := processing.NewDataMapShards(
		poolCtx,
		viper.GetInt("workers.sysmon.correlate"),
		"sysmon events",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer shardsSysmon.Close()

	wiseCorrelationCh := make(chan processing.EncodedEntry)
	defer close(wiseCorrelationCh)
	wiseConnectionCh := make(chan processing.EncodedEntry)
	defer close(wiseConnectionCh)

	if err := processing.OutputWISE(processing.WiseConfig{
		ConfigStreamWorkers: processing.ConfigStreamWorkers{
			Ctx:    poolCtx,
			Logger: log,
			Pool:   pool,
		},
		ForwardNetworkEvents: viper.GetBool("wise.connections.enabled"),
		ClientOnlyNetwork: redis.NewClient(&redis.Options{
			Addr:     viper.GetString("wise.connections.redis.host"),
			DB:       viper.GetInt("wise.connections.redis.db"),
			Password: viper.GetString("wise.connections.redis.password"),
		}),
		ChanOnlyNetwork: wiseConnectionCh,
		ClientCorrelated: redis.NewClient(&redis.Options{
			Addr:     viper.GetString("wise.correlations.redis.host"),
			DB:       viper.GetInt("wise.correlations.redis.db"),
			Password: viper.GetString("wise.correlations.redis.password"),
		}),
		ChanCorrelated: wiseCorrelationCh,
	}); err != nil {
		log.Fatal(err)
	}

	keys := viper.GetStringSlice("suricata.redis.key.input")
	suriShardsCorrelations := make([]*processing.DataMapShards, 0)
	for _, key := range keys {
		suriShard, err := processing.NewDataMapShards(
			poolCtx,
			viper.GetInt("workers.suricata.correlate"),
			"correlations to suricata queue "+key,
		)
		if err != nil {
			log.Fatal(err)
		}
		defer suriShard.Close()
		suriShardsCorrelations = append(suriShardsCorrelations, suriShard)
	}

	if err := processing.CorrelateSysmonEvents(processing.SysmonCorrelateConfig{
		ConfigStreamWorkers: processing.ConfigStreamWorkers{
			Name:    "correlate sysmon",
			Workers: viper.GetInt("workers.sysmon.correlate"),
			Pool:    pool,
			Ctx:     poolCtx,
			Logger:  log,
		},
		Shards:          shardsSysmon,
		LogCorrelations: viper.GetBool("general.log.correlations"),
		WinlogConfig: processing.WinlogConfig{
			StoreNetEvents:       true,
			WorkDir:              viper.GetString("general.work_dir"),
			ChanCorrelated:       wiseCorrelationCh,
			ChanOnlyNetwork:      wiseConnectionCh,
			ForwardNetworkEvents: viper.GetBool("wise.connections.enabled"),
			Buckets: processing.WinlogBucketsConfig{
				Command: processing.BucketsConfig{
					Count: viper.GetInt("sysmon.buckets.process.count"),
					Size:  viper.GetDuration("sysmon.buckets.process.duration"),
				},
				Network: processing.BucketsConfig{
					Count: viper.GetInt("sysmon.buckets.connection.count"),
					Size:  viper.GetDuration("sysmon.buckets.connection.duration"),
				},
			},
			SuricataHandler: func() []processing.MapHandlerFunc {
				if len(suriShardsCorrelations) == 0 {
					return nil
				}
				handlers := make([]processing.MapHandlerFunc, 0)
				for _, shard := range suriShardsCorrelations {
					balancerCorrelations, err := shard.Handler("network", "community_id")
					if err != nil {
						log.Fatal(err)
					}
					handlers = append(handlers, balancerCorrelations)
				}
				return handlers
			}(),
		},
	}); err != nil {
		log.Fatal(err)
	}

	balancerSysmon, err := shardsSysmon.Handler("process", "entity_id")
	if err != nil {
		log.Fatal(err)
	}
	if err := processing.ConsumeRedis(processing.ConfigConsumeRedis{
		ConfigStreamRedis: processing.ConfigStreamRedis{
			Client: redis.NewClient(&redis.Options{
				Addr:     viper.GetString("sysmon.redis.host"),
				DB:       viper.GetInt("sysmon.redis.db"),
				Password: viper.GetString("sysmon.redis.password"),
			}),
			Key: viper.GetString("sysmon.redis.key"),
		},
		Handler: balancerSysmon,
		ConfigStreamWorkers: processing.ConfigStreamWorkers{
			Name:    "consume sysmon",
			Workers: viper.GetInt("workers.sysmon.consume"),
			Pool:    pool,
			Ctx:     poolCtx,
			Logger:  log,
		},
	}); err != nil {
		log.Fatal(err)
	}

	if viper.GetBool("suricata.enabled") {
		keys := viper.GetStringSlice("suricata.redis.key.input")
		if len(keys) == 0 {
			log.Fatal("Suricata redis key missing")
		}
		for i, keyInput := range keys {
			shardsSuricata, err := processing.NewDataMapShards(
				poolCtx,
				viper.GetInt("workers.suricata.correlate"),
				fmt.Sprintf("suricata events - %s", keyInput),
			)
			if err != nil {
				log.Fatal(err)
			}
			defer shardsSuricata.Close()

			if err := processing.CorrelateSuricataEvents(processing.SuricataCorrelateConfig{
				ConfigStreamWorkers: processing.ConfigStreamWorkers{
					Name:    "correlate suricata",
					Workers: viper.GetInt("workers.suricata.correlate"),
					Pool:    pool,
					Ctx:     poolCtx,
					Logger:  log,
				},
				InputEventShards:      shardsSuricata,
				CorrelatedEventShards: suriShardsCorrelations[i],
				Output: processing.ConfigStreamRedis{
					Client: redis.NewClient(&redis.Options{
						Addr:     viper.GetString("suricata.redis.host"),
						DB:       viper.GetInt("suricata.redis.db"),
						Password: viper.GetString("suricata.redis.password"),
					}),
					Key: func() string {
						if suff := viper.GetString("suricata.redis.key.output_suffix"); suff != "" {
							return keyInput + "_" + suff
						}
						if output := viper.GetString("suricata.redis.key.output"); output != "" {
							return output
						}
						log.Fatal(errors.New("missing redis output key for suricata"))
						return ""
					}(),
				},
			}); err != nil {
				log.Fatal(err)
			}

			balancerSuricata, err := shardsSuricata.Handler("community_id")
			if err != nil {
				log.Fatal(err)
			}
			if err := processing.ConsumeRedis(processing.ConfigConsumeRedis{
				ConfigStreamRedis: processing.ConfigStreamRedis{
					Client: redis.NewClient(&redis.Options{
						Addr:     viper.GetString("suricata.redis.host"),
						DB:       viper.GetInt("suricata.redis.db"),
						Password: viper.GetString("suricata.redis.password"),
					}),
					Key: keyInput,
				},
				Handler: balancerSuricata,
				ConfigStreamWorkers: processing.ConfigStreamWorkers{
					Name:    "consume suricata",
					Workers: viper.GetInt("workers.suricata.consume"),
					Pool:    pool,
					Ctx:     poolCtx,
					Logger:  log,
				},
			}); err != nil {
				log.Fatal(err)
			}
		}
	}

	if err := pool.Wait(); err != nil {
		log.Fatal(err)
	}
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the main enrichment procedure",
	Long: `This command enriches network events by correlating sysmon command and network
  events via community ID enrichment.

  pikksilm run`,
	Run: run,
}

func init() {
	rootCmd.AddCommand(runCmd)

	pFlags := runCmd.PersistentFlags()

	// runtime and workers setup
	pFlags.String("work-dir", "/var/lib/pikksilm", "Working directory. Used for persistence. Pikksilm needs write access")
	viper.BindPFlag("general.work_dir", pFlags.Lookup("work-dir"))

	pFlags.Bool("log-correlations", false, "Log correlated events to per-worker log file. Mainly for debugging.")
	viper.BindPFlag("general.log.correlations", pFlags.Lookup("log-correlations"))

	// sysmon consumer
	pFlags.String("sysmon-redis-host", "localhost:6379", "Redis host to consume sysmon from.")
	viper.BindPFlag("sysmon.redis.host", pFlags.Lookup("sysmon-redis-host"))

	pFlags.Int("sysmon-redis-db", 0, "Redis database for sysmon consumer.")
	viper.BindPFlag("sysmon.redis.db", pFlags.Lookup("sysmon-redis-db"))

	pFlags.String("sysmon-redis-password", "", "Password for sysmon redis instance. Empty value disables authentication.")
	viper.BindPFlag("sysmon.redis.password", pFlags.Lookup("sysmon-redis-password"))

	pFlags.Int("sysmon-buckets-process-count", 4, "Number of buckets for storing event_id 1 events.")
	viper.BindPFlag("sysmon.buckets.process.count", pFlags.Lookup("sysmon-buckets-process-count"))

	pFlags.Duration("sysmon-buckets-process-duration", 15*time.Minute, "Size of event_id 1 bucket.")
	viper.BindPFlag("sysmon.buckets.process.duration", pFlags.Lookup("sysmon-buckets-process-duration"))

	pFlags.Int("sysmon-buckets-connection-count", 2, "Number of buckets for storing event_id 3 events.")
	viper.BindPFlag("sysmon.buckets.connection.count", pFlags.Lookup("sysmon-buckets-connection-count"))

	pFlags.Duration("sysmon-buckets-connection-duration", 15*time.Second, "Size of event_id 3 bucket.")
	viper.BindPFlag("sysmon.buckets.connection.duration", pFlags.Lookup("sysmon-buckets-connection-duration"))

	pFlags.String("sysmon-redis-key", "winlogbeat", "Redis key for winlogbeat messages.")
	viper.BindPFlag("sysmon.redis.key", pFlags.Lookup("sysmon-redis-key"))

	// workers setup
	pFlags.Int("workers-sysmon-consume", 2, "Number of workers for sysmon consume and JSON decode.")
	viper.BindPFlag("workers.sysmon.consume", pFlags.Lookup("workers-sysmon-consume"))

	pFlags.Int("workers-sysmon-correlate", 2, "Number of workers for sysmon correlation.")
	viper.BindPFlag("workers.sysmon.correlate", pFlags.Lookup("workers-sysmon-correlate"))

	pFlags.Int("workers-suricata-consume", 2, "Number of workers consuming Suricata events")
	viper.BindPFlag("workers.suricata.consume", pFlags.Lookup("workers-suricata-consume"))

	pFlags.Int("workers-suricata-correlate", 2, "Number of workers consuming Suricata correlation")
	viper.BindPFlag("workers.suricata.correlate", pFlags.Lookup("workers-suricata-correlate"))

	// wise output - correlations
	pFlags.String("wise-correlations-redis-host", "localhost:6379", "Redis host output correlations for WISE. Event ID 1 + 3.")
	viper.BindPFlag("wise.correlations.redis.host", pFlags.Lookup("wise-correlations-redis-host"))

	pFlags.Int("wise-correlations-redis-db", 1, "Redis database for WISE correlations.")
	viper.BindPFlag("wise.correlations.redis.db", pFlags.Lookup("wise-correlations-redis-db"))

	pFlags.String("wise-correlations-redis-password", "", "Password for WISE Redis correlations. Empty value disables authentication.")
	viper.BindPFlag("wise.correlations.redis.password", pFlags.Lookup("wise-correlations-redis-password"))

	// wise output - only connections, no correlated process creation info
	pFlags.Bool("wise-connection-enabled", false, "Enable forwarding of raw network events with not ID 1 correlation.")
	viper.BindPFlag("wise.connections.enabled", pFlags.Lookup("wise-connection-enabled"))

	pFlags.String("wise-connections-redis-host", "localhost:6379", "Redis host output connections for WISE. Only event ID 3.")
	viper.BindPFlag("wise.connections.redis.host", pFlags.Lookup("wise-connections-redis-host"))

	pFlags.Int("wise-connections-redis-db", 2, "Redis database for WISE connections.")
	viper.BindPFlag("wise.connections.redis.db", pFlags.Lookup("wise-connections-redis-db"))

	pFlags.String("wise-connections-redis-password", "", "Password for WISE Redis connections. Empty value disables authentication.")
	viper.BindPFlag("wise.connections.redis.password", pFlags.Lookup("wise-connections-redis-password"))

	// suricata parameters
	pFlags.Bool("suricata-enabled", false, "Enable suricata correlation")
	viper.BindPFlag("suricata.enabled", pFlags.Lookup("suricata-enabled"))

	pFlags.String("suricata-redis-host", "localhost:6379", "Redis host for EVE stream.")
	viper.BindPFlag("suricata.redis.host", pFlags.Lookup("suricata-redis-host"))

	pFlags.Int("suricata-redis-db", 0, "Redis database EVE stream.")
	viper.BindPFlag("suricata.redis.db", pFlags.Lookup("suricata-redis-db"))

	pFlags.String("suricata-redis-password", "", "Password for EVE stream. Empty value disables authentication.")
	viper.BindPFlag("suricata.redis.password", pFlags.Lookup("suricata-redis-password"))

	pFlags.StringSlice("suricata-redis-key-input", []string{"suricata"}, "Redis key for EVE stream.")
	viper.BindPFlag("suricata.redis.key.input", pFlags.Lookup("suricata-redis-key-input"))

	pFlags.String("suricata-redis-key-output", "suricata_edr", "Redis key for EVE stream.")
	viper.BindPFlag("suricata.redis.key.output", pFlags.Lookup("suricata-redis-key-output"))

	pFlags.String("suricata-redis-key-output-suffix", "", "Redis key for EVE stream.")
	viper.BindPFlag("suricata.redis.key.output_suffix", pFlags.Lookup("suricata-redis-key-output-suffix"))
}
