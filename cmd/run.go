package cmd

import (
	"context"
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

	shards, err := processing.NewDataMapShards(poolCtx, viper.GetInt("workers.sysmon.correlate"))
	if err != nil {
		log.Fatal(err)
	}
	defer shards.Close()

	wiseCorrelationCh := make(chan processing.EncodedEntry)
	defer close(wiseCorrelationCh)
	wiseConnectionCh := make(chan processing.EncodedEntry)
	defer close(wiseConnectionCh)

	if err := processing.OutputWISE(processing.WiseConfig{
		Ctx:                  poolCtx,
		Logger:               log,
		Pool:                 pool,
		ForwardNetworkEvents: viper.GetBool("wise.connections.redis.enabled"),
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

	if err := processing.CorrelateSysmonEvents(processing.SysmonCorrelateConfig{
		Workers: viper.GetInt("workers.sysmon.correlate"),
		Pool:    pool,
		Ctx:     poolCtx,
		Logger:  log,
		Shards:  shards,
		WinlogConfig: processing.WinlogConfig{
			StoreNetEvents:       true,
			WorkDir:              viper.GetString("general.work_dir"),
			ChanCorrelated:       wiseCorrelationCh,
			ChanOnlyNetwork:      wiseConnectionCh,
			ForwardNetworkEvents: viper.GetBool("wise.connections.redis.enabled"),
			Buckets: processing.WinlogBucketsConfig{
				Command: processing.BucketsConfig{
					Count: 4,
					Size:  15 * time.Minute,
				},
				Network: processing.BucketsConfig{
					Count: 2,
					Size:  15 * time.Second,
				},
			},
		},
	}); err != nil {
		log.Fatal(err)
	}

	if err := processing.ConsumeSysmonEvents(processing.SysmonConsumeConfig{
		Client: redis.NewClient(&redis.Options{
			Addr:     viper.GetString("sysmon.redis.host"),
			DB:       viper.GetInt("sysmon.redis.db"),
			Password: viper.GetString("sysmon.redis.password"),
		}),
		Key:     viper.GetString("sysmon.redis.key"),
		Workers: viper.GetInt("workers.sysmon.consume"),
		Pool:    pool,
		Ctx:     poolCtx,
		Logger:  log,
		Handler: shards.Handler(),
	}); err != nil {
		log.Fatal(err)
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

	// sysmon consumer
	pFlags.String("sysmon-redis-host", "localhost:6379", "Redis host to consume sysmon from.")
	viper.BindPFlag("sysmon.redis.host", pFlags.Lookup("sysmon-redis-host"))

	pFlags.Int("sysmon-redis-db", 0, "Redis database for sysmon consumer.")
	viper.BindPFlag("sysmon.redis.db", pFlags.Lookup("sysmon-redis-db"))

	pFlags.String("sysmon-redis-password", "", "Password for sysmon redis instance. Empty value disables authentication.")
	viper.BindPFlag("sysmon.redis.password", pFlags.Lookup("sysmon-redis-password"))

	pFlags.String("sysmon-redis-key", "winlogbeat", "Redis key for winlogbeat messages.")
	viper.BindPFlag("sysmon.redis.key", pFlags.Lookup("sysmon-redis-key"))

	// workers setup
	pFlags.Int("workers-sysmon-consume", 2, "Number of workers for sysmon consume and JSON decode.")
	viper.BindPFlag("workers.sysmon.consume", pFlags.Lookup("workers-sysmon-consume"))

	pFlags.Int("workers-sysmon-correlate", 2, "Number of workers for sysmon correlation.")
	viper.BindPFlag("workers.sysmon.correlate", pFlags.Lookup("workers-sysmon-correlate"))

	// wise output - correlations
	pFlags.String("wise-correlations-redis-host", "localhost:6379", "Redis host output correlations for WISE. Event ID 1 + 3.")
	viper.BindPFlag("wise.correlations.redis.host", pFlags.Lookup("wise-correlations-redis-host"))

	pFlags.Int("wise-correlations-redis-db", 1, "Redis database for WISE correlations.")
	viper.BindPFlag("wise.correlations.redis.db", pFlags.Lookup("wise-correlations-redis-db"))

	pFlags.String("wise-correlations-redis-password", "", "Password for WISE Redis correlations. Empty value disables authentication.")
	viper.BindPFlag("wise.correlations.redis.password", pFlags.Lookup("wise-correlations-redis-password"))

	// wise output - only connections, no correlated process creation info
	pFlags.Bool("wise-connection-redis-enabled", false, "Enable forwarding of raw network events with not ID 1 correlation.")
	viper.BindPFlag("wise.connections.redis.enabled", pFlags.Lookup("wise-connection-redis-enabled"))

	pFlags.String("wise-connections-redis-host", "localhost:6379", "Redis host output connections for WISE. Only event ID 3.")
	viper.BindPFlag("wise.connections.redis.host", pFlags.Lookup("wise-connections-redis-host"))

	pFlags.Int("wise-connections-redis-db", 2, "Redis database for WISE connections.")
	viper.BindPFlag("wise.connections.redis.db", pFlags.Lookup("wise-connections-redis-db"))

	pFlags.String("wise-connections-redis-password", "", "Password for WISE Redis connections. Empty value disables authentication.")
	viper.BindPFlag("wise.connections.redis.password", pFlags.Lookup("wise-connections-redis-password"))
}
