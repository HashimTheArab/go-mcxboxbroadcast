package main

import (
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"golang.org/x/term"
)

func newLogHandler(out io.Writer, level slog.Leveler, color bool) slog.Handler {
	if color {
		return tint.NewTextHandler(out, &tint.Options{
			Level:       level,
			TimeFormat:  time.TimeOnly,
			ReplaceAttr: colorLogLevel,
		})
	}
	return slog.NewTextHandler(out, &slog.HandlerOptions{Level: level})
}

func colorLogLevel(groups []string, attr slog.Attr) slog.Attr {
	if len(groups) != 0 || attr.Key != slog.LevelKey {
		return attr
	}
	level, ok := attr.Value.Any().(slog.Level)
	if !ok {
		return attr
	}
	color := uint8(2) // green
	switch {
	case level >= slog.LevelError:
		color = 1 // red
	case level >= slog.LevelWarn:
		color = 11 // bright yellow
	case level >= slog.LevelInfo:
		color = 14 // bright cyan
	}
	return tint.Attr(color, attr)
}

func shouldColorLogs(out io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if force := os.Getenv("FORCE_COLOR"); force != "" {
		return force != "0"
	}
	f, ok := out.(*os.File)
	if !ok {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}
