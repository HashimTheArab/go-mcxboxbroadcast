package broadcaster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func TestPterodactylArtifacts(t *testing.T) {
	eggData, err := os.ReadFile("deployments/pterodactyl/egg-go-mcxboxbroadcast.json")
	if err != nil {
		t.Fatal(err)
	}

	var egg struct {
		Meta struct {
			Version string `json:"version"`
		} `json:"meta"`
		Name         string            `json:"name"`
		DockerImages map[string]string `json:"docker_images"`
		Startup      string            `json:"startup"`
		Config       struct {
			Files   string `json:"files"`
			Startup string `json:"startup"`
			Logs    string `json:"logs"`
			Stop    string `json:"stop"`
		} `json:"config"`
		Scripts struct {
			Installation struct {
				Script     string `json:"script"`
				Container  string `json:"container"`
				Entrypoint string `json:"entrypoint"`
			} `json:"installation"`
		} `json:"scripts"`
		Variables []struct {
			EnvVariable string `json:"env_variable"`
		} `json:"variables"`
	}
	if err := json.Unmarshal(eggData, &egg); err != nil {
		t.Fatalf("parse egg json: %v", err)
	}
	if egg.Meta.Version != "PTDL_v2" {
		t.Fatalf("egg version = %q, want PTDL_v2", egg.Meta.Version)
	}
	if egg.Name != "go-mcxboxbroadcast" {
		t.Fatalf("egg name = %q, want go-mcxboxbroadcast", egg.Name)
	}
	if got := egg.DockerImages["go-mcxboxbroadcast (Pterodactyl)"]; got != "ghcr.io/hashimthearab/go-mcxboxbroadcast:pterodactyl" {
		t.Fatalf("pterodactyl image = %q", got)
	}
	if egg.Startup != "/mcxboxbroadcast -config /home/container/config.yml" {
		t.Fatalf("startup = %q", egg.Startup)
	}
	if egg.Config.Stop != "^C" {
		t.Fatalf("stop command = %q, want ^C", egg.Config.Stop)
	}
	if egg.Scripts.Installation.Container != "ghcr.io/pterodactyl/installers:alpine" {
		t.Fatalf("install container = %q", egg.Scripts.Installation.Container)
	}
	if egg.Scripts.Installation.Entrypoint != "ash" {
		t.Fatalf("install entrypoint = %q", egg.Scripts.Installation.Entrypoint)
	}
	for name, raw := range map[string]string{
		"config.files":   egg.Config.Files,
		"config.startup": egg.Config.Startup,
		"config.logs":    egg.Config.Logs,
	} {
		var parsed any
		if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
			t.Fatalf("%s is not valid json: %v", name, err)
		}
	}

	var seenTargetHost, seenTargetPort, seenICEPortMin, seenICEPortMax bool
	for _, variable := range egg.Variables {
		seenTargetHost = seenTargetHost || variable.EnvVariable == "TARGET_SERVER_HOST"
		seenTargetPort = seenTargetPort || variable.EnvVariable == "TARGET_SERVER_PORT"
		seenICEPortMin = seenICEPortMin || variable.EnvVariable == "ICE_PORT_MIN"
		seenICEPortMax = seenICEPortMax || variable.EnvVariable == "ICE_PORT_MAX"
	}
	if !seenTargetHost || !seenTargetPort {
		t.Fatalf("target server variables missing: host=%v port=%v", seenTargetHost, seenTargetPort)
	}
	if !seenICEPortMin || !seenICEPortMax {
		t.Fatalf("ICE port variables missing: min=%v max=%v", seenICEPortMin, seenICEPortMax)
	}
	for _, want := range []string{"ICE_PORT_MIN", "ICE_PORT_MAX", "session.icePortRange.min", "session.icePortRange.max"} {
		if !strings.Contains(egg.Config.Files, want) {
			t.Fatalf("config.files does not contain %q", want)
		}
	}
	for _, want := range []string{"icePortRange:", "${ICE_PORT_MIN}", "${ICE_PORT_MAX}"} {
		if !strings.Contains(egg.Scripts.Installation.Script, want) {
			t.Fatalf("installation script does not contain %q", want)
		}
	}
	if strings.Contains(egg.Scripts.Installation.Script, "signalingMode:") {
		t.Fatal("installation script should rely on the default signaling mode")
	}

	dockerfile, err := os.ReadFile("Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	dockerfileText := string(dockerfile)
	for _, want := range []string{
		"FROM runtime-base AS pterodactyl",
		"adduser -S -G container -h /home/container container",
		"ENV USER=container HOME=/home/container",
		"WORKDIR /home/container",
		"COPY deployments/pterodactyl/entrypoint.sh /entrypoint.sh",
		"FROM runtime-base AS standalone",
	} {
		if !strings.Contains(dockerfileText, want) {
			t.Fatalf("Dockerfile does not contain %q", want)
		}
	}

	workflow, err := os.ReadFile(".github/workflows/docker.yml")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(workflow), "$DOCKER_IMAGE:pterodactyl") {
		t.Fatal("docker workflow does not publish the pterodactyl tag")
	}
}

// pterodactylEgg holds the egg fields that must track the program's behaviour.
type pterodactylEgg struct {
	Config struct {
		Startup string `json:"startup"`
	} `json:"config"`
	Scripts struct {
		Installation struct {
			Script string `json:"script"`
		} `json:"installation"`
	} `json:"scripts"`
}

func loadPterodactylEgg(t *testing.T) pterodactylEgg {
	t.Helper()
	data, err := os.ReadFile("deployments/pterodactyl/egg-go-mcxboxbroadcast.json")
	if err != nil {
		t.Fatal(err)
	}
	var egg pterodactylEgg
	if err := json.Unmarshal(data, &egg); err != nil {
		t.Fatal(err)
	}
	return egg
}

// The panel shows the server as starting until the egg's done marker appears in the log.
func TestPterodactylDoneMarkerIsLoggedOnStart(t *testing.T) {
	var startup struct {
		Done string `json:"done"`
	}
	if err := json.Unmarshal([]byte(loadPterodactylEgg(t).Config.Startup), &startup); err != nil {
		t.Fatal(err)
	}
	if startup.Done == "" {
		t.Fatal("egg has no startup.done marker")
	}
	var logs bytes.Buffer
	b, err := New(Config{
		Server:         ServerInfo{Host: "127.0.0.1", Port: 19132},
		XBLTokenSource: staticTokenSource{xuid: "123"},
		Signaling:      &fakeSignaling{networkID: "123456789"},
		ListenConfig:   minecraft.ListenConfig{AuthenticationDisabled: true},
		Status:         Status{HostName: "Host", WorldName: "World"},
		UpdateInterval: time.Hour,
		Log:            slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	b.announcerFactory = func(*Broadcaster) room.Announcer { return &fakeAnnouncer{} }
	if err := b.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), startup.Done) {
		t.Fatalf("startup never logged egg marker %q; logs:\n%s", startup.Done, logs.String())
	}
}

// Panel values containing YAML syntax must reach the config verbatim and load strictly.
func TestPterodactylInstallerEscapesPanelValues(t *testing.T) {
	shell, err := exec.LookPath("sh")
	if err != nil {
		t.Skip("no POSIX shell")
	}
	dir := t.TempDir()
	script := strings.Replace(loadPterodactylEgg(t).Scripts.Installation.Script, "cd /mnt/server", "cd "+dir, 1)
	hostName := `My "Bedrock" Server's #1 \n: {x}`
	cmd := exec.Command(shell, "-c", script)
	cmd.Env = append(os.Environ(),
		"DEBUG_MODE=false", "QUERY_SERVER=true", "CONFIG_FALLBACK=false",
		"ICE_PORT_MIN=0", "ICE_PORT_MAX=0",
		"HOST_NAME="+hostName, "WORLD_NAME=World: 'one'", "MAX_PLAYERS=20",
		"TARGET_SERVER_HOST=bedrock.test", "TARGET_SERVER_PORT=19133",
		"NOTIFICATIONS_ENABLED=false", "WEBHOOK_URL=",
		"GALLERY_ENABLED=true", "GALLERY_IMAGE_PATH=screenshot.jpg",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("installer failed: %v\n%s", err, out)
	}
	path := filepath.Join(dir, "config.yml")
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("configVersion: %d\n", CurrentConfigVersion); !bytes.HasPrefix(written, []byte(want)) {
		t.Fatalf("installer config does not start with %q", want)
	}
	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Notes) != 0 {
		t.Fatalf("installer config needed adjustments: %v", cfg.Notes)
	}
	info := cfg.Session.SessionInfo
	if info.HostName != hostName || info.WorldName != "World: 'one'" || info.IP != "bedrock.test" || info.Port != 19133 {
		t.Fatalf("installer wrote sessionInfo %+v", info)
	}
}
