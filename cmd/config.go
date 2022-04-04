package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// configCmd represents the config command
var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Generate default config",
	Run: func(cmd *cobra.Command, args []string) {
		log.WithField("path", cfgFile).Infof("Writing config")
		viper.WriteConfigAs(cfgFile)
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
}
