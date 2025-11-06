package cmd

import (
	"github.com/markuskont/pikksilm/processing/v2"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Generate default config",
	Run: func(cmd *cobra.Command, args []string) {
		processing.Logger.Info("writing config", "path", cfgFile)
		viper.WriteConfigAs(cfgFile)
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
}
