package cmd

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	processing "github.com/markuskont/pikksilm/processing/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

func run(cmd *cobra.Command, args []string) {
	if viper.GetBool("log.debug") {
		processing.LogLevel.Set(slog.LevelDebug)
	}

	defer func() {
		processing.Logger.Info("good exit")
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
		processing.Logger.Warn("interrupt caught")
		stop()
		return nil
	})

	eventsSysmon := make(processing.HandleSysmonCoreBlocking, viper.GetInt("process.sysmon.buffer"))
	defer close(eventsSysmon)

	confPool := processing.ConfigWorkerPool{
		Pool:        pool,
		Ctx:         ctx,
		LogInterval: viper.GetDuration("log.interval"),
	}

	confSysmonConsume := &processing.ConfigConsume{}
	confSysmonConsume.ConfigWorkerPool = confPool

	confSysmonConsume.TX = eventsSysmon.Func()

	confSysmonConsume.Redis = processing.ConfigRedis{
		Key:      viper.GetString("input.sysmon.redis.key"),
		Addr:     viper.GetString("input.sysmon.redis.host"),
		DB:       viper.GetInt("input.sysmon.redis.db"),
		Password: viper.GetString("input.sysmon.redis.password"),
	}

	if err := processing.Consume(*confSysmonConsume); err != nil {
		processing.Logger.Error(err.Error())
		os.Exit(1)
	}

	confSysmonProcess := &processing.ConfigProcessWinlog{}
	confSysmonProcess.ConfigWorkerPool = confPool

	confSysmonProcess.RX = eventsSysmon
	confSysmonProcess.CacheSize = viper.GetInt("process.sysmon.cache")
	confSysmonProcess.Handers = make([]processing.HandleWinlog, 0)

	if p := viper.GetString("output.correlations.file.path"); viper.GetBool("output.correlations.file.enabled") && p != "" {
		log := processing.Logger.With("path", p)
		log.Debug("appending to file")
		h, err := processing.NewWriterFile(p)
		if err != nil {
			processing.Logger.Error(err.Error())
			os.Exit(1)
		}
		defer func() {
			if err := h.Close(); err != nil {
				log.Error(err.Error())
			}
			log.Debug("closing file")
		}()
		confSysmonProcess.Handers = append(confSysmonProcess.Handers, h.FuncWinlog())
	}

	if viper.GetBool("process.suricata.enabled") {
		processing.Logger.Debug("streaming Suricata EVE")

		eventsSuricata := make(processing.HandleDecodeGeneric, viper.GetInt("process.suricata.buffer"))
		defer close(eventsSuricata)

		confSuricataConsume := &processing.ConfigConsume{}
		confSuricataConsume.ConfigWorkerPool = confPool

		confSuricataConsume.TX = eventsSuricata.Func()

		confSuricataConsume.Redis = processing.ConfigRedis{
			Key:      viper.GetString("input.suricata.redis.key"),
			Addr:     viper.GetString("input.suricata.redis.host"),
			DB:       viper.GetInt("input.suricata.redis.db"),
			Password: viper.GetString("input.suricata.redis.password"),
		}

		if err := processing.Consume(*confSuricataConsume); err != nil {
			processing.Logger.Error(err.Error())
			os.Exit(1)
		}

		confSuricataProcess := &processing.ConfigProcessSuricata{}
		confSuricataProcess.ConfigWorkerPool = confPool

		confSuricataProcess.RX.Events = eventsSuricata

		h := processing.NewHandleBridge(confPool.Ctx, viper.GetInt("process.suricata.buffer"))
		confSuricataProcess.RX.Correlations = h.RX()
		confSysmonProcess.Handers = append(confSysmonProcess.Handers, h.FuncWinlog())

		confSuricataProcess.Cache = viper.GetInt("process.suricata.cache")
		confSuricataProcess.BulkSize = viper.GetInt("process.suricata.bulk")
		confSuricataProcess.Delay = viper.GetDuration("process.suricata.delay")

		if p := viper.GetString("output.suricata.file.path"); viper.GetBool("output.suricata.file.enabled") && p != "" {
			log := processing.Logger.With("path", p)
			log.Debug("appending to file")
			h, err := processing.NewWriterFile(p)
			if err != nil {
				processing.Logger.Error(err.Error())
				os.Exit(1)
			}
			defer func() {
				if err := h.Close(); err != nil {
					log.Error(err.Error())
				}
				log.Debug("closing file")
			}()
			confSuricataProcess.Handlers = append(confSuricataProcess.Handlers, h.FuncEncoded())
		}

		if viper.GetBool("output.suricata.redis.enabled") {
			processing.Logger.Debug("starting Suricata Redis output handler")
			h, err := processing.NewWriterRedis(processing.ConfigRedis{
				Key:      viper.GetString("output.suricata.redis.key"),
				Addr:     viper.GetString("output.suricata.redis.host"),
				DB:       viper.GetInt("output.suricata.redis.db"),
				Password: viper.GetString("output.suricata.redis.password"),
			})
			if err != nil {
				processing.Logger.Error(err.Error())
				os.Exit(1)
			}
			confSuricataProcess.Handlers = append(confSuricataProcess.Handlers, h.FuncEncodedBulk())
		}

		if err := processing.ProcessSuricata(*confSuricataProcess); err != nil {
			processing.Logger.Error(err.Error())
			os.Exit(1)
		}
	}

	if viper.GetBool("output.correlations.redis.enabled") {
		processing.Logger.Debug("starting Arkime WISE redis handler")
		h, err := processing.NewWriterRedis(processing.ConfigRedis{
			DynKey:   true,
			Addr:     viper.GetString("output.correlations.redis.host"),
			DB:       viper.GetInt("output.correlations.redis.db"),
			Password: viper.GetString("output.correlations.redis.password"),
		})
		if err != nil {
			processing.Logger.Error(err.Error())
			os.Exit(1)
		}
		confSysmonProcess.Handers = append(confSysmonProcess.Handers, h.FuncWinlog())
	}

	if err := processing.ProcessWinlog(*confSysmonProcess); err != nil {
		processing.Logger.Error(err.Error())
		os.Exit(1)
	}

	if err := pool.Wait(); err != nil {
		processing.Logger.Error(err.Error())
		os.Exit(1)
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

	register := []string{
		"log-interval",
		"log-debug",
		"input-sysmon-redis-host",
		"input-sysmon-redis-db",
		"input-sysmon-redis-password",
		"input-sysmon-redis-key",
		"input-suricata-redis-host",
		"input-suricata-redis-db",
		"input-suricata-redis-password",
		"input-suricata-redis-key",
		"process-sysmon-buffer",
		"process-sysmon-cache",
		"process-suricata-enabled",
		"process-suricata-buffer",
		"process-suricata-cache",
		"process-suricata-bulk",
		"process-suricata-delay",
		"output-correlations-redis-enabled",
		"output-correlations-redis-host",
		"output-correlations-redis-db",
		"output-correlations-redis-password",
		"output-correlations-file-enabled",
		"output-correlations-file-path",
		"output-suricata-file-enabled",
		"output-suricata-file-path",
		"output-suricata-redis-enabled",
		"output-suricata-redis-host",
		"output-suricata-redis-db",
		"output-suricata-redis-password",
		"output-suricata-redis-key",
	}

	pFlags.Duration("log-interval", 30*time.Second, "Periodic logging")
	pFlags.Bool("log-debug", false, "Increase logging verbosity")

	pFlags.String("input-sysmon-redis-host", "localhost:6379", "Redis host to consume sysmon from.")
	pFlags.Int("input-sysmon-redis-db", 0, "Redis database for sysmon consumer.")
	pFlags.String("input-sysmon-redis-password", "", "Password for sysmon redis instance. Empty value disables authentication.")
	pFlags.String("input-sysmon-redis-key", "winlogbeat", "Redis key for winlogbeat messages.")

	pFlags.String("input-suricata-redis-host", "localhost:6379", "Redis host to consume suricata from.")
	pFlags.Int("input-suricata-redis-db", 0, "Redis database for suricata consumer.")
	pFlags.String("input-suricata-redis-password", "", "Password for suricata redis instance. Empty value disables authentication.")
	pFlags.String("input-suricata-redis-key", "suricata", "Redis key for suricata messages.")

	pFlags.Int("process-sysmon-buffer", 10000, "Buffer size for internal message queue")
	pFlags.Int("process-sysmon-cache", 1000000, "Cache size for processing sysmon streams")

	pFlags.Bool("process-suricata-enabled", false, "Enable Suricata processing")
	pFlags.Int("process-suricata-buffer", 10000, "Buffer size for internal message queue")
	pFlags.Int("process-suricata-cache", 100000, "Number of sysmon correlations to cache")
	pFlags.Int("process-suricata-bulk", 100000, "Maximum number of items to store in delay bulk.")
	pFlags.Duration("process-suricata-delay", 1*time.Second, "Suricata events are stored in delay bulk. That bulk will be processed at this interval.")

	pFlags.Bool("output-correlations-redis-enabled", false, "Push correlations to Arkime WISE via Redis")
	pFlags.String("output-correlations-redis-host", "localhost:6379", "Redis host and port.")
	pFlags.Int("output-correlations-redis-db", 1, "Redis database for wise producer.")
	pFlags.String("output-correlations-redis-password", "", "Password for wise redis instance. Empty value disables authentication.")

	pFlags.Bool("output-correlations-file-enabled", false, "Enable sysmon correlation log file output")
	pFlags.String("output-correlations-file-path", "", "Log file for sysmon correlations")

	pFlags.Bool("output-suricata-file-enabled", false, "Enable Suricata EVE log file output")
	pFlags.String("output-suricata-file-path", "", "Log file for Suricata EVE")

	pFlags.Bool("output-suricata-redis-enabled", false, "Push Suricata EVE to Redis")
	pFlags.String("output-suricata-redis-host", "localhost:6379", "Redis host to consume wise from.")
	pFlags.Int("output-suricata-redis-db", 1, "Redis database for Suricata")
	pFlags.String("output-suricata-redis-password", "", "Password for Suricata EVE instance. Empty value disables authentication.")
	pFlags.String("output-suricata-redis-key", "suricata", "Redis key for suricata messages.")

	for _, flg := range register {
		if err := viper.BindPFlag(strings.ReplaceAll(flg, "-", "."), pFlags.Lookup(flg)); err != nil {
			panic(err)
		}
	}
}
