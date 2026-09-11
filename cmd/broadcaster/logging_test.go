package main

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestLogHandlerUsesColorOnlyWhenEnabled(t *testing.T) {
	for _, tt := range []struct {
		name  string
		color bool
	}{
		{"structured", false},
		{"terminal", true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			log := slog.New(newLogHandler(&out, slog.LevelInfo, tt.color))
			log.Info("created xbox live session", "target", "example.org:19132")

			got := out.String()
			if strings.Contains(got, "\x1b[") != tt.color {
				t.Fatalf("ANSI color present = %t, want %t: %q", strings.Contains(got, "\x1b["), tt.color, got)
			}
			if !tt.color && (!strings.Contains(got, "level=INFO") || !strings.Contains(got, "msg=\"created xbox live session\"")) {
				t.Fatalf("non-terminal output is not structured slog text: %q", got)
			}
			if !strings.Contains(got, "created xbox live session") || !strings.Contains(got, "example.org:19132") {
				t.Fatalf("log output lost message or attributes: %q", got)
			}
		})
	}
}

func TestShouldColorLogsHonorsEnvironment(t *testing.T) {
	var out bytes.Buffer
	t.Setenv("NO_COLOR", "")
	t.Setenv("FORCE_COLOR", "")
	if shouldColorLogs(&out) {
		t.Fatal("non-terminal output enabled color")
	}
	t.Setenv("FORCE_COLOR", "1")
	if !shouldColorLogs(&out) {
		t.Fatal("FORCE_COLOR did not enable color")
	}
	t.Setenv("NO_COLOR", "1")
	if shouldColorLogs(&out) {
		t.Fatal("NO_COLOR did not disable color")
	}
	t.Setenv("NO_COLOR", "")
	t.Setenv("FORCE_COLOR", "0")
	if shouldColorLogs(&out) {
		t.Fatal("FORCE_COLOR=0 did not disable color")
	}
}

func TestShouldColorLogsRejectsNonTerminalCharacterDevice(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("FORCE_COLOR", "")
	f, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if shouldColorLogs(f) {
		t.Fatal("non-terminal character device enabled color")
	}
}

func TestLogHandlerUsesJavaLevelColors(t *testing.T) {
	var out bytes.Buffer
	log := slog.New(newLogHandler(&out, slog.LevelDebug, true))
	log.Debug("debug")
	log.Info("info")
	log.Warn("warn")
	log.Error("error")

	got := out.String()
	for _, want := range []string{"\x1b[32mDBG", "\x1b[96mINF", "\x1b[93mWRN", "\x1b[31mERR"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing level color %q in %q", want, got)
		}
	}
}
