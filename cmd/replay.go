package cmd

import (
	"bufio"
	"os"
	"time"

	"github.com/markuskont/pikksilm/pkg/models"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
)

// replayCmd represents the replay command
var replayCmd = &cobra.Command{
	Use:   "replay",
	Short: "Replay log events to simulate stream.",
	Long: `This subcommand reads winlogbeat events from static files and outputs to simulated stream.
  It expects EventID 1 and EventID 3 events to be in seprate log files in winlogbeat format (7.x).
  We assume that log files are already sorted. Command will skip to first event with ID 1 (command)
  and then write events to combined output with more or less accurate time deltas between events.

  pikksilm replay --log-event-id-3 event-id-3.json --log-event-id-1 event-id-1.json`,

	Run: func(cmd *cobra.Command, args []string) {
		// channel to communicate command log beginning to second worker
		chTimeStart := make(chan time.Time)
		defer close(chTimeStart)

		// channel to block first worker until second is in same timeframe
		chReady := make(chan struct{})
		defer close(chReady)

		keyTS := viper.GetString("replay.key.timestamp")
		// open handle to event id 1 and scan to first applicable event
		pool := errgroup.Group{}

		pool.Go(func() error {
			pth := viper.GetString("replay.log.event_id_1")
			logrus.
				WithField("path", pth).
				Info("Scanning command logs.")

			f, err := os.Open(pth)
			if err != nil {
				return err
			}
			defer f.Close()

			w := os.Stdout
			defer w.Close()

			var last time.Time

			var count int
			scanner := bufio.NewScanner(f)
		loop:
			for scanner.Scan() {
				var e models.Entry

				if err := models.Decoder.Unmarshal(scanner.Bytes(), &e); err != nil {
					return err
				}
				ts, ok, err := e.GetTimestamp(keyTS)
				if err != nil {
					return err
				}
				if !ok {
					logrus.
						WithField("raw", scanner.Text()).
						WithField("key", keyTS).
						WithField("event_id", 1).
						Warning("timestamp not found")
					continue loop
				}
				// check first event timestamp and communicate it to network event worker
				if count == 0 {
					logrus.WithField("event_id", 1).Debug("sending first timestamp")
					// communicate timestamp to network log worker
					chTimeStart <- ts
					// block until it has finished scanning to same timeframe
					// TODO - add select and configurable timeout
					logrus.WithField("event_id", 1).Debug("waiting for sync")
					<-chReady
				} else if delay := ts.Sub(last); !last.IsZero() && delay > 10*time.Microsecond {
					time.Sleep(delay)
				}
				last = ts

				w.Write(scanner.Bytes())
				count++
			}
			return scanner.Err()
		})

		pool.Go(func() error {
			pth := viper.GetString("replay.log.event_id_3")
			logrus.
				WithField("path", pth).
				Info("Scanning network logs.")

			f, err := os.Open(pth)
			if err != nil {
				return err
			}
			defer f.Close()

			w := os.Stdout
			defer w.Close()

			var last time.Time
			var found bool

			// pull first timestamp of command logs
			first := <-chTimeStart

			var count int
			scanner := bufio.NewScanner(f)
		loop:
			for scanner.Scan() {
				var e models.Entry

				if err := models.Decoder.Unmarshal(scanner.Bytes(), &e); err != nil {
					return err
				}
				ts, ok, err := e.GetTimestamp(keyTS)
				if err != nil {
					return err
				}
				if !ok {
					logrus.
						WithField("raw", scanner.Text()).
						WithField("key", keyTS).
						WithField("event_id", 3).
						Warning("timestamp not found")
					continue loop
				}

				if ts.Before(first) {
					continue loop
				}
				if !found {
					logrus.
						WithField("offset", count).
						WithField("event_id", 3).
						Info("skip scan done")
						// notify previous worker that we're good to go
					chReady <- struct{}{}
					found = true
				}

				if delay := ts.Sub(last); !last.IsZero() && delay > 10*time.Microsecond {
					time.Sleep(delay)
				}
				last = ts

				w.Write(scanner.Bytes())
				count++
			}
			return scanner.Err()
		})

		if err := pool.Wait(); err != nil {
			logrus.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(replayCmd)

	pFlags := replayCmd.PersistentFlags()

	pFlags.String("log-event-id-3", "./event-id-3.json", "Full path pointing to log file with event-id 3.")
	viper.BindPFlag("replay.log.event_id_3", pFlags.Lookup("log-event-id-3"))

	pFlags.String("log-event-id-1", "./event-id-1.json", "Full path pointing to log file with event-id 1.")
	viper.BindPFlag("replay.log.event_id_1", pFlags.Lookup("log-event-id-1"))

	pFlags.String("key-timestamp", "@timestamp", "Change timestamp JSON key if needed.")
	viper.BindPFlag("replay.key.timestamp", pFlags.Lookup("key-timestamp"))
}
