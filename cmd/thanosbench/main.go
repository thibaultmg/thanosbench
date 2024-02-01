package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"github.com/alecthomas/kingpin/v2"
	// "github.com/go-kit/log"
	// "github.com/go-kit/log/level"
	"github.com/oklog/run"
	"github.com/prometheus/common/version"
)

const (
	logFormatLogfmt = "logfmt"
	logFormatJson   = "json"
)

type setupFunc func(*run.Group, *slog.Logger) error

func main() {
	if os.Getenv("DEBUG") != "" {
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(10)
	}

	app := kingpin.New(filepath.Base(os.Args[0]), "Benchmarking tools for Thanos")

	app.Version(version.Print("thanosbench"))
	app.HelpFlag.Short('h')

	debugName := app.Flag("debug.name", "Name to add as prefix to log lines.").Hidden().String()

	logLevel := app.Flag("log.level", "Log filtering level.").
		Default("info").Enum("error", "warn", "info", "debug")
	logFormat := app.Flag("log.format", "Log format to use.").
		Default(logFormatLogfmt).Enum(logFormatLogfmt, logFormatJson)

	cmds := map[string]setupFunc{}
	// registerWalgen(cmds, app)
	registerBlock(cmds, app)
	// registerStress(cmds, app)

	cmd, err := app.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, fmt.Errorf("error parsing commandline arguments: %w", err))
		app.Usage(os.Args[1:])
		os.Exit(2)
	}

	// var logger *slog.Logger
	// var logger log.Logger
	lvl := new(slog.LevelVar)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	{
		switch *logLevel {
		case "error":
			lvl.Set(slog.LevelError)
		case "warn":
			lvl.Set(slog.LevelWarn)
		case "info":
			lvl.Set(slog.LevelInfo)
		case "debug":
			lvl.Set(slog.LevelDebug)
		default:
			panic("unexpected log level")
		}
		if *logFormat == logFormatJson {
			logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
		}

		if *debugName != "" {
			logger = logger.With("name", *debugName)
			// logger = log.With(logger, "name", *debugName)
		}

		// logger = logger.With("ts", slog.DefaultTimestampUTC, "caller", slog.DefaultCaller)

		// logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	}

	var g run.Group
	if err := cmds[cmd](&g, logger); err != nil {
		logger.Error(fmt.Sprintf("%s command failed", cmd), "err", err)
		// level.Error(logger).Log("err", fmt.Sprintf("%v", fmt.Errorf("%s command failed; %w", cmd, err)))
		os.Exit(1)
	}

	// Listen for termination signals.
	{
		cancel := make(chan struct{})
		g.Add(func() error {
			return interrupt(logger, cancel)
		}, func(error) {
			close(cancel)
		})
	}

	if err := g.Run(); err != nil {
		logger.Error("running command failed", "err", err)
		os.Exit(1)
	}
	if cmd != "block plan" {
		logger.Info("exiting", "cmd", cmd)
	}
}

func interrupt(logger *slog.Logger, cancel <-chan struct{}) error {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-c:
		logger.Info("caught signal. Exiting.", "signal", s)
		return nil
	case <-cancel:
		return errors.New("cancelled")
	}
}
