package cmd

import (
	"bufio"
	"os"

	"github.com/markuskont/pikksilm/pkg/enrich"
	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the main enrichment procedure",
	Long: `This command enriches network events by correlating sysmon command and network
  events via community ID enrichment.

  pikksilm run`,
	Run: func(cmd *cobra.Command, args []string) {
		c, _ := enrich.NewCorrelate()
		r := os.Stdin
		scanner := bufio.NewScanner(r)
		var count int
		for scanner.Scan() {
			var e models.Entry
			if err := models.Decoder.Unmarshal(scanner.Bytes(), &e); err != nil {
				logrus.Fatal(err)
			}
			if err := c.Winlog(e); err != nil {
				logrus.Error(err)
			}
			count++
		}
		if err := scanner.Err(); err != nil {
			logrus.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(runCmd)
}
