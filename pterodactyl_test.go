package broadcaster

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
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
		t.Fatal("installation script should not expose a signaling mode")
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
	if !strings.Contains(string(workflow), "${{ env.DOCKER_IMAGE }}:pterodactyl") {
		t.Fatal("docker workflow does not publish the pterodactyl tag")
	}
}
