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

	eventsSysmon := make(processing.HandleSysmonCoreBlocking, viper.GetInt("sysmon.buffer"))
	defer close(eventsSysmon)

	poolConf := processing.ConfigWorkerPool{
		Pool:        pool,
		Ctx:         ctx,
		LogInterval: viper.GetDuration("log.interval"),
	}

	confSysmonConsume := &processing.ConfigConsume{}
	confSysmonConsume.ConfigWorkerPool = poolConf

	confSysmonConsume.TX = eventsSysmon.Func()

	confSysmonConsume.Redis.Key = viper.GetString("sysmon.redis.key")
	confSysmonConsume.Redis.Addr = viper.GetString("sysmon.redis.host")
	confSysmonConsume.Redis.DB = viper.GetInt("sysmon.redis.db")
	confSysmonConsume.Redis.Password = viper.GetString("sysmon.redis.password")

	if err := processing.Consume(*confSysmonConsume); err != nil {
		processing.Logger.Error(err.Error())
		os.Exit(1)
	}

	confSysmonProcess := &processing.ConfigWinlogProcess{}
	confSysmonProcess.ConfigWorkerPool = poolConf

	confSysmonProcess.RX = eventsSysmon
	confSysmonProcess.CacheSize = viper.GetInt("sysmon.cache")
	confSysmonProcess.Handers = make([]processing.HandleWinlog, 0)

	if p := viper.GetString("output.file.correlations"); p != "" {
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
		confSysmonProcess.Handers = append(confSysmonProcess.Handers, h.Func())
	}

	if viper.GetBool("wise.enabled") {
		h, err := processing.NewWriterWISE(processing.ConfigRedis{
			DynKey:   true,
			Addr:     viper.GetString("wise.redis.host"),
			DB:       viper.GetInt("wise.redis.db"),
			Password: viper.GetString("wise.redis.password"),
		})
		if err != nil {
			processing.Logger.Error(err.Error())
			os.Exit(1)
		}
		confSysmonProcess.Handers = append(confSysmonProcess.Handers, h.Func())
	}

	if err := processing.WinlogProcess(*confSysmonProcess); err != nil {
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
		"sysmon-redis-host",
		"sysmon-redis-db",
		"sysmon-redis-password",
		"sysmon-redis-key",
		"sysmon-buffer",
		"sysmon-cache",
		"output-file-correlations",
		"wise-enabled",
		"wise-redis-host",
		"wise-redis-db",
		"wise-redis-password",
	}

	pFlags.Duration("log-interval", 30*time.Second, "Periodic logging")
	pFlags.Bool("log-debug", false, "Increase logging verbosity")

	pFlags.String("sysmon-redis-host", "localhost:6379", "Redis host to consume sysmon from.")
	pFlags.Int("sysmon-redis-db", 0, "Redis database for sysmon consumer.")
	pFlags.String("sysmon-redis-password", "", "Password for sysmon redis instance. Empty value disables authentication.")
	pFlags.String("sysmon-redis-key", "winlogbeat", "Redis key for winlogbeat messages.")
	pFlags.Int("sysmon-buffer", 1000, "Buffer size for internal message queue")
	pFlags.Int("sysmon-cache", 1000000, "Cache size for processing sysmon streams")

	pFlags.Bool("wise-enabled", false, "Push correlations to Arkime WISE via Redis")
	pFlags.String("wise-redis-host", "localhost:6379", "Redis host to consume wise from.")
	pFlags.Int("wise-redis-db", 1, "Redis database for wise consumer.")
	pFlags.String("wise-redis-password", "", "Password for wise redis instance. Empty value disables authentication.")

	pFlags.String("output-file-correlations", "", "Log file for sysmon correlations")

	for _, flg := range register {
		if err := viper.BindPFlag(strings.ReplaceAll(flg, "-", "."), pFlags.Lookup(flg)); err != nil {
			panic(err)
		}
	}
}
