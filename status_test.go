package broadcaster

import (
	"context"
	"testing"
)

func TestStatusDefaults(t *testing.T) {
	b, err := New(Config{
		TokenSource:     staticTokenSource{},
		LiveTokenSource: staticOAuthSource{},
		Server:          ServerInfo{Host: "127.0.0.1", Port: 19132},
		Status: Status{
			HostName:   "§aHost",
			WorldName:  "",
			Players:    0,
			MaxPlayers: 0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := b.status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.HostName != "Host" {
		t.Fatalf("unexpected host name %q", status.HostName)
	}
	if status.WorldName != "Host" {
		t.Fatalf("unexpected world name %q", status.WorldName)
	}
	if status.MemberCount != 1 {
		t.Fatalf("unexpected member count %d", status.MemberCount)
	}
	if status.MaxMemberCount != 2 {
		t.Fatalf("unexpected max member count %d", status.MaxMemberCount)
	}
}
