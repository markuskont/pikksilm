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
		c, err := enrich.NewCorrelate(enrich.CorrelateConfig{
			BucketCount:  viper.GetInt("run.buckets.count"),
			BucketSize:   viper.GetDuration("run.buckets.size"),
			LookupWindow: viper.GetDuration("run.buckets.lookup"),
		})
		if err != nil {
			log.Fatal(err)
		}
		r := os.Stdin
		scanner := bufio.NewScanner(r)
		tick := time.NewTicker(3 * time.Second)
		defer tick.Stop()
		var count int
		for scanner.Scan() {
			select {
			case <-tick.C:
				log.
					WithField("enriched", c.Enriched).
					WithField("emitted", c.Sent).
					WithField("dropped", c.Dropped).
					WithField("count", count).
					WithField("enriched_percent", float64(c.Enriched)/float64(count)).
					Info("enrichment report")
			default:
			}
			var e models.Entry
			if err := models.Decoder.Unmarshal(scanner.Bytes(), &e); err != nil {
				log.Fatal(err)
			}
			if err := c.Process(e); err != nil {
				log.Error(err)
			}
			count++
		}
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	pFlags := runCmd.PersistentFlags()

	pFlags.Int("buckets-count", 4, "Number of date buckets kept in memory")
	viper.BindPFlag("run.buckets.count", pFlags.Lookup("buckets-count"))

	pFlags.Duration("buckets-size", 15*time.Second, "Bucket size. Smaller number means more lookups with granular retention.")
	viper.BindPFlag("run.buckets.size", pFlags.Lookup("buckets-size"))

	pFlags.Duration("buckets-lookup", 30*time.Second, "Sliding window lookup size.")
	viper.BindPFlag("run.buckets.lookup", pFlags.Lookup("buckets-lookup"))
}
