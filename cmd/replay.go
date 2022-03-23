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

		var beginning time.Time
		if b := viper.GetString("replay.time.beginning"); b != "" {
			t, err := time.Parse(models.ArgTimeFormat, b)
			if err != nil {
				logrus.Fatal(err)
			}
			beginning = t
		}

		keyTS := viper.GetString("replay.key.timestamp")
		// open handle to event id 1 and scan to first applicable event
		pool := errgroup.Group{}
		// modifier to speed up or slow down the replay
		modifier := viper.GetFloat64("replay.time.modifier")

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

			r := bufio.NewReader(f)

			w := os.Stdout
			defer w.Close()

			var last time.Time
			var found bool

			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			start := time.Now()

			if !beginning.IsZero() {
				chTimeStart <- beginning
			}
			var count int
			var written int
			scanner := bufio.NewScanner(r)
		loop:
			for scanner.Scan() {
				var e models.Entry

				select {
				case <-ticker.C:
					logrus.
						WithField("eps", float64(count)/time.Since(start).Seconds()).
						WithField("event_id", 1).
						WithField("last", last).
						WithField("written", written).
						Info("scanning")
				default:
				}

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
				if count == 0 && beginning.IsZero() {
					logrus.WithField("event_id", 1).Debug("sending first timestamp")
					// communicate timestamp to network log worker
					chTimeStart <- ts
					// block until it has finished scanning to same timeframe
					// TODO - add select and configurable timeout
					logrus.WithField("event_id", 1).Debug("waiting for sync")
					<-chReady
				} else if !beginning.IsZero() && !found {
					if ts.Before(beginning) {
						count++
						last = ts
						continue loop
					}
					// ts after beginning but not found yet, wait for other worker to be ready
					logrus.
						WithField("event_id", 1).
						WithField("offset", count).
						WithField("last", last).
						Info("found beginning, waiting for sync")
					<-chReady
					found = true
					logrus.
						WithField("event_id", 1).
						Info("sync done")
				}

				replaySleep(ts, last, modifier)
				last = ts

				w.Write(append(scanner.Bytes(), []byte("\n")...))
				written++
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

			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()
			start := time.Now()

			var count int
			var written int
			scanner := bufio.NewScanner(f)
		loop:
			for scanner.Scan() {
				var e models.Entry

				select {
				case <-ticker.C:
					logrus.
						WithField("eps", float64(count)/time.Since(start).Seconds()).
						WithField("event_id", 3).
						WithField("last", last).
						WithField("written", written).
						Info("scanning")
				default:
				}

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
					count++
					last = ts
					continue loop
				}
				if !found {
					logrus.
						WithField("offset", count).
						WithField("event_id", 3).
						WithField("last", last).
						Info("skip scan done")

					// notify previous worker that we're good to go
					chReady <- struct{}{}
					found = true
				}

				replaySleep(ts, last, modifier)
				last = ts

				w.Write(append(scanner.Bytes(), []byte("\n")...))
				written++
				count++
			}
			return scanner.Err()
		})

		if err := pool.Wait(); err != nil {
			logrus.Fatal(err)
		}
	},
}

func replaySleep(ts, last time.Time, modifier float64) {
	if delay := ts.Sub(last); !last.IsZero() && delay > 1*time.Microsecond {
		delay = time.Duration(float64(delay.Nanoseconds()) / modifier)
		time.Sleep(delay)
	}
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

	pFlags.String("time-beginning", "", "Set a explicit replay beginning")
	viper.BindPFlag("replay.time.beginning", pFlags.Lookup("time-beginning"))

	pFlags.Float64("time-modifier", 1.0, "Simple coefficient to speed up or slow down replay.")
	viper.BindPFlag("replay.time.modifier", pFlags.Lookup("time-modifier"))
}
