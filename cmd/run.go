package cmd

import (
	"bufio"
	"os"
	"time"

	"github.com/markuskont/pikksilm/pkg/enrich"
	"github.com/markuskont/pikksilm/pkg/models"
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
		r := os.Stdin
		scanner := bufio.NewScanner(r)
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
		if err := scanner.Err(); err != nil {
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
}
