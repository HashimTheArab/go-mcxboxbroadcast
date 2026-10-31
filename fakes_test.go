package broadcaster

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/xal/xasd"
	"github.com/df-mc/go-xsapi/v2/xal/xasu"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func response(code int, body string) *http.Response {
	return responseBytes(code, []byte(body))
}

func responseBytes(code int, body []byte) *http.Response {
	return &http.Response{
		StatusCode: code,
		Status:     http.StatusText(code),
		Body:       io.NopCloser(bytes.NewReader(body)),
		Header:     make(http.Header),
	}
}

// offlineHTTPClient fails every request, so tests never reach the network.
func offlineHTTPClient() *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("offline")
	})}
}

func testAuthenticatedClient(auth string, next http.RoundTripper) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		req = req.Clone(req.Context())
		req.Header.Set("Authorization", auth)
		return next.RoundTrip(req)
	})}
}

func testBroadcasterLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// testTitleEndpoints routes every Xbox Live host through NSAL with an XBL token.
const testTitleEndpoints = `{"EndPoints":[{"Protocol":"https","Host":"*.xboxlive.com","HostType":"wildcard","RelyingParty":"http://xboxlive.com","TokenType":"JWT"}]}`

// newTestXSAPIClient builds real API subclients for xuid that send requests through client.
func newTestXSAPIClient(t *testing.T, client *http.Client, xuid string) *xsapi.Client {
	t.Helper()
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	configured := *client
	configured.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == "title.mgt.xboxlive.com" {
			// NSAL caches the default title process-wide, so every test must stub the same one.
			return response(http.StatusOK, testTitleEndpoints), nil
		}
		return base.RoundTrip(req)
	})
	xbl, err := (xsapi.ClientConfig{HTTPClient: &configured, RTAMode: xsapi.RTADisabled}).New(t.Context(), staticTokenSource{xuid: xuid})
	if err != nil {
		t.Fatalf("create xsapi client: %v", err)
	}
	return xbl
}

// testXBLClient is a real Xbox Live client for xuid whose Xbox requests all fail.
func testXBLClient(t *testing.T, xuid string) *xsapi.Client {
	t.Helper()
	return newTestXSAPIClient(t, offlineHTTPClient(), xuid)
}

// staticTokenSource issues Xbox tokens for xuid, or "1" when empty, without network calls.
type staticTokenSource struct {
	xuid string
}

func (s staticTokenSource) XSTSToken(context.Context, string) (*xsts.Token, error) {
	xuid := s.xuid
	if xuid == "" {
		xuid = "1"
	}
	return &xsts.Token{
		Token:    "token",
		NotAfter: time.Now().Add(time.Hour),
		DisplayClaims: xsts.DisplayClaims{UserInfo: []xsts.UserInfo{{
			UserInfo: xasu.UserInfo{UserHash: "user"},
			XUID:     xuid,
			GamerTag: "Tester",
		}}},
	}, nil
}

func (staticTokenSource) DeviceToken(context.Context) (*xasd.Token, error) {
	return &xasd.Token{
		Token:         "device",
		NotAfter:      time.Now().Add(time.Hour),
		DisplayClaims: xasd.DisplayClaims{DeviceInfo: xasd.DeviceInfo{DeviceID: "device"}},
	}, nil
}

func (staticTokenSource) ProofKey() *ecdsa.PrivateKey { return testProofKey() }

var testProofKey = sync.OnceValue(func() *ecdsa.PrivateKey {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(err)
	}
	return key
})

type staticMinecraftTokenSource struct{}

func (staticMinecraftTokenSource) ServiceToken(context.Context) (*service.Token, error) {
	return &service.Token{AuthorizationHeader: "Bearer minecraft", ValidUntil: time.Now().Add(time.Hour)}, nil
}

// minecraftTokenSourceWithPMID issues tokens whose claims carry a Player Messaging identity.
type minecraftTokenSourceWithPMID struct {
	pmid uuid.UUID
}

func (s minecraftTokenSourceWithPMID) ServiceToken(context.Context) (*service.Token, error) {
	payload, err := json.Marshal(map[string]string{"pmid": s.pmid.String()})
	if err != nil {
		return nil, err
	}
	return &service.Token{
		AuthorizationHeader: "Bearer header." + base64.RawURLEncoding.EncodeToString(payload) + ".signature",
		ValidUntil:          time.Now().Add(time.Hour),
		Claims:              service.Claims{PlayerMessagingID: s.pmid},
	}, nil
}

type fakeNotifier struct {
	notify func(context.Context, string)
}

func (f fakeNotifier) Notify(ctx context.Context, message string) error {
	if f.notify != nil {
		f.notify(ctx, message)
	}
	return nil
}

type fakeAnnouncer struct {
	mu          sync.Mutex
	announceErr error
	closeErr    error
	closed      bool
	status      room.Status
}

func (f *fakeAnnouncer) Announce(_ context.Context, status room.Status) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
	return f.announceErr
}

func (f *fakeAnnouncer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return f.closeErr
}

func (f *fakeAnnouncer) Status() room.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *fakeAnnouncer) Closed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

// hidingAnnouncer is a published session with a member count that records when it is hidden.
type hidingAnnouncer struct {
	fakeAnnouncer
	hid     bool
	members int // negative means unpublished
}

func (a *hidingAnnouncer) memberCount() (int, bool, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return max(a.members, 0), a.members >= 0, false
}

func (a *hidingAnnouncer) setMembers(n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.members = n
}

func (a *hidingAnnouncer) hide(context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.hid = true
	return nil
}

func (a *hidingAnnouncer) hidden() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.hid
}

// fakeSignaling is a JSON-RPC signaling connection that is lost when ctx ends.
type fakeSignaling struct {
	ctx       context.Context // nil never ends
	networkID string          // empty is "123456789"
	pmid      uuid.UUID       // nil is fakePlayerMessagingID
	closed    atomic.Bool
}

var fakePlayerMessagingID = uuid.MustParse("11111111-2222-3333-4444-555555555555")

func (f *fakeSignaling) PlayerMessagingID() uuid.UUID {
	if f.pmid != uuid.Nil {
		return f.pmid
	}
	return fakePlayerMessagingID
}

func (f *fakeSignaling) Signal(context.Context, *nethernet.Signal) error { return nil }

func (f *fakeSignaling) Notify(nethernet.Notifier) func() { return func() {} }

func (f *fakeSignaling) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeSignaling) Credentials(context.Context) (*nethernet.Credentials, error) {
	return nil, nil
}

func (f *fakeSignaling) NetworkID() string {
	if f.networkID != "" {
		return f.networkID
	}
	return "123456789"
}

func (f *fakeSignaling) PongData([]byte) {}

func (f *fakeSignaling) Close() error {
	f.closed.Store(true)
	return nil
}

// syncHistoryStore keeps player history in memory; onLastSeen, when set, observes each lookup.
type syncHistoryStore struct {
	mu         sync.Mutex
	lastSeen   map[string]time.Time
	onLastSeen func(xuid string)
}

func (s *syncHistoryStore) LastSeen(_ context.Context, xuid string) (time.Time, bool, error) {
	if s.onLastSeen != nil {
		s.onLastSeen(xuid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	when, ok := s.lastSeen[xuid]
	return when, ok, nil
}

func (s *syncHistoryStore) Seen(_ context.Context, xuid string, when time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastSeen == nil {
		s.lastSeen = map[string]time.Time{}
	}
	s.lastSeen[xuid] = when
	return nil
}

func (s *syncHistoryStore) Clear(_ context.Context, xuid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.lastSeen, xuid)
	return nil
}

func (s *syncHistoryStore) XUIDs(context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	xuids := make([]string, 0, len(s.lastSeen))
	for xuid := range s.lastSeen {
		xuids = append(xuids, xuid)
	}
	return xuids, nil
}
