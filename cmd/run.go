package cmd

import (
	"time"

	"github.com/markuskont/pikksilm/pkg/enrich"
	"github.com/markuskont/pikksilm/pkg/stream"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the main enrichment procedure",
	Long: `This command enriches network events by correlating sysmon command and network
  events via community ID enrichment.

  pikksilm run`,
	Run: func(cmd *cobra.Command, args []string) {
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
		})
		if err != nil {
			log.Fatal(err)
		}
		if err := stream.ReadWinlogStdin(log, c); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	pFlags := runCmd.PersistentFlags()

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

	pFlags.String("redis-host", "localhost:6379", "Host to redis instance.")
	viper.BindPFlag("run.redis.host", pFlags.Lookup("redis-host"))

	pFlags.Int("redis-winlog-db", 0, "Redis database for ingest events.")
	viper.BindPFlag("run.redis.winlog.db", pFlags.Lookup("redis-winlog-db"))

	pFlags.String("redis-winlog-key", "winlogbeat", "Key to consume windows logs from")
	viper.BindPFlag("run.redis.winlog.key", pFlags.Lookup("redis-winlog-key"))

	pFlags.Int("redis-wise-db", 0, "Redis database for WISE output")
	viper.BindPFlag("run.redis.wise.db", pFlags.Lookup("redis-wise-db"))
}
