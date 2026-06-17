package broadcaster

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

func TestBroadcasterStartSubAccountsMutuallyFollowsBeforePublish(t *testing.T) {
	var calls []string
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls = append(calls, fmt.Sprintf("%s %s auth=%s", req.Method, req.URL.String(), req.Header.Get("Authorization")))
		return broadcasterResponse(http.StatusNoContent, ""), nil
	})}
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient:  &xsapi.Client{},
		XUID:       "100",
		HTTPClient: client,
		SubAccounts: []SubAccountConfig{{
			ID:        "sub",
			Enabled:   true,
			XBLClient: &xsapi.Client{},
			XUID:      "200",
		}},
	}}
	b.subAccountPublisher = func(context.Context, SubAccountConfig, mpsd.SessionReference, mpsd.PublishConfig) (*mpsd.Session, error) {
		calls = append(calls, "publish")
		return &mpsd.Session{}, nil
	}

	if err := b.startSubAccounts(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"PUT https://social.xboxlive.com/users/me/people/xuid(200) auth=",
		"PUT https://social.xboxlive.com/users/me/people/xuid(100) auth=",
		"publish",
	}
	if fmt.Sprint(calls) != fmt.Sprint(want) {
		t.Fatalf("unexpected call order\n got: %v\nwant: %v", calls, want)
	}
}

func TestBroadcasterStartSubAccountsSkipsMutualFollowWithoutXUIDs(t *testing.T) {
	var httpCalls, publishCalls int
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		XBLClient: &xsapi.Client{},
		HTTPClient: &http.Client{Transport: broadcasterRoundTripFunc(func(*http.Request) (*http.Response, error) {
			httpCalls++
			return broadcasterResponse(http.StatusNoContent, ""), nil
		})},
		SubAccounts: []SubAccountConfig{{
			ID:        "sub",
			Enabled:   true,
			XBLClient: &xsapi.Client{},
			XUID:      "200",
		}},
	}}
	b.subAccountPublisher = func(context.Context, SubAccountConfig, mpsd.SessionReference, mpsd.PublishConfig) (*mpsd.Session, error) {
		publishCalls++
		return &mpsd.Session{}, nil
	}

	if err := b.startSubAccounts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if httpCalls != 0 {
		t.Fatalf("expected no follow requests without both XUIDs, got %d", httpCalls)
	}
	if publishCalls != 1 {
		t.Fatalf("expected sub-account publish to continue, got %d calls", publishCalls)
	}
}

func TestBroadcasterTransferSendsStartGameBeforeTransfer(t *testing.T) {
	conn := &recordingTransferConn{}
	b := &Broadcaster{log: testBroadcasterLogger(), conf: Config{
		Server: ServerInfo{Host: "play.example.net", Port: 19133},
		Status: Status{WorldName: "Redirect Lobby"},
	}}

	b.transfer(conn)

	startGameIndex, transferIndex := -1, -1
	for i, pk := range conn.packets {
		switch pk.(type) {
		case *packet.StartGame:
			if startGameIndex == -1 {
				startGameIndex = i
			}
		case *packet.Transfer:
			transferIndex = i
		}
	}
	if startGameIndex == -1 {
		t.Fatal("StartGame was not sent")
	}
	if transferIndex == -1 {
		t.Fatal("Transfer was not sent")
	}
	if startGameIndex > transferIndex {
		t.Fatalf("StartGame sent after Transfer: startGame=%d transfer=%d", startGameIndex, transferIndex)
	}
	transfer := conn.packets[transferIndex].(*packet.Transfer)
	if transfer.Address != "play.example.net" || transfer.Port != 19133 {
		t.Fatalf("unexpected transfer target %#v", transfer)
	}
	if conn.flushes != 1 {
		t.Fatalf("expected one flush, got %d", conn.flushes)
	}
	if !conn.closed {
		t.Fatal("connection was not closed")
	}
}

type broadcasterRoundTripFunc func(*http.Request) (*http.Response, error)

func (f broadcasterRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func broadcasterResponse(code int, body string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

func testBroadcasterLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type recordingTransferConn struct {
	packets []packet.Packet
	flushes int
	closed  bool
}

func (c *recordingTransferConn) WritePacket(pk packet.Packet) error {
	c.packets = append(c.packets, pk)
	return nil
}

func (c *recordingTransferConn) Flush() error {
	c.flushes++
	return nil
}

func (c *recordingTransferConn) Close() error {
	c.closed = true
	return nil
}

func (c *recordingTransferConn) IdentityData() login.IdentityData {
	return login.IdentityData{XUID: "visitor", DisplayName: "Visitor"}
}
