package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/df-mc/go-xsapi/v2/rta"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

// fakeXbox serves RTA (a WebSocket at rta.xboxlive.com) and MPSD REST for
// one session at a time, so tests drive the real go-xsapi clients.
type fakeXbox struct {
	srv *httptest.Server

	mu         sync.Mutex
	ws         *websocket.Conn
	exists     bool
	deleted    bool
	name       string
	created    []string
	custom     json.RawMessage
	customPUTs int
	memberPUTs int
	failCustom int           // the next N custom property PUTs return 500
	hangCustom chan struct{} // when set, custom property PUTs block until closed
	failClose  bool
	subscribed chan struct{}
}

func newFakeXbox(t *testing.T) *fakeXbox {
	f := &fakeXbox{subscribed: make(chan struct{}, 16)}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() {
		f.mu.Lock()
		if f.hangCustom != nil {
			select {
			case <-f.hangCustom:
			default:
				close(f.hangCustom)
			}
		}
		f.mu.Unlock()
		f.srv.CloseClientConnections()
		f.srv.Close()
	})
	return f
}

// httpClient routes every Xbox host to the fake server.
func (f *fakeXbox) httpClient() *http.Client {
	target, _ := url.Parse(f.srv.URL)
	return &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		req = req.Clone(req.Context())
		req.Header.Set("X-Orig-Host", req.URL.Host)
		req.URL.Scheme, req.URL.Host, req.Host = "http", target.Host, target.Host
		return http.DefaultTransport.RoundTrip(req)
	})}
}

func (f *fakeXbox) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("X-Orig-Host") == "rta.xboxlive.com" {
		f.serveRTA(w, r)
		return
	}
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/handles"):
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	case r.Method == http.MethodGet:
		f.mu.Lock()
		gone := f.deleted || !f.exists || !strings.EqualFold(path.Base(r.URL.Path), f.name)
		body := f.bodyLocked()
		f.mu.Unlock()
		if gone {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", fmt.Sprint(len(body)))
		_, _ = w.Write(body)
	case r.Method == http.MethodPut:
		f.servePut(w, r)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeXbox) servePut(w http.ResponseWriter, r *http.Request) {
	var d struct {
		Properties *struct {
			Custom json.RawMessage `json:"custom"`
		} `json:"properties"`
		Members map[string]json.RawMessage `json:"members"`
	}
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &d)
	f.mu.Lock()
	if r.Header.Get("If-None-Match") == "*" {
		f.exists, f.deleted, f.name = true, false, path.Base(r.URL.Path)
		f.created = append(f.created, f.name)
		if d.Properties != nil {
			f.custom = d.Properties.Custom
		}
		body := f.bodyLocked()
		f.mu.Unlock()
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body)
		return
	}
	if f.deleted || !f.exists || !strings.EqualFold(path.Base(r.URL.Path), f.name) {
		f.mu.Unlock()
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	if me, ok := d.Members["me"]; ok && string(me) == "null" {
		if f.failClose {
			f.mu.Unlock()
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		f.deleted = true
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if _, ok := d.Members["me"]; ok {
		f.memberPUTs++
	}
	if d.Properties != nil && d.Properties.Custom != nil {
		f.customPUTs++
		hang := f.hangCustom
		if f.failCustom > 0 {
			f.failCustom--
			f.mu.Unlock()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		f.mu.Unlock()
		if hang != nil {
			select {
			case <-hang:
			case <-r.Context().Done():
				return
			}
		}
		f.mu.Lock()
		f.custom = d.Properties.Custom
	}
	body := f.bodyLocked()
	f.mu.Unlock()
	_, _ = w.Write(body)
}

// bodyLocked returns the session document: the owner plus one joined player.
func (f *fakeXbox) bodyLocked() []byte {
	custom := f.custom
	if custom == nil {
		custom = json.RawMessage(`{}`)
	}
	member := func(xuid string) map[string]any {
		return map[string]any{
			"constants":  map[string]any{"system": map[string]any{"xuid": xuid}},
			"properties": map[string]any{"system": map[string]any{"active": true}},
		}
	}
	body, _ := json.Marshal(map[string]any{
		"properties": map[string]any{"system": map[string]any{"joinRestriction": "followed", "readRestriction": "followed"}, "custom": custom},
		"members":    map[string]any{"0": member("100"), "1": member("200")},
	})
	return body
}

func (f *fakeXbox) serveRTA(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{"rta.xboxlive.com.V2"}})
	if err != nil {
		return
	}
	f.mu.Lock()
	f.ws = c
	f.mu.Unlock()
	for {
		var msg []json.RawMessage
		if err := wsjson.Read(context.Background(), c, &msg); err != nil {
			return
		}
		var typ, seq uint32
		_ = json.Unmarshal(msg[0], &typ)
		_ = json.Unmarshal(msg[1], &seq)
		switch typ {
		case 1:
			_ = wsjson.Write(context.Background(), c, []any{1, seq, 0, 1, map[string]string{"ConnectionId": uuid.NewString()}})
			select {
			case f.subscribed <- struct{}{}:
			default:
			}
		case 2:
			_ = wsjson.Write(context.Background(), c, []any{2, seq, 0})
		}
	}
}

// tap sends an RTA shoulder tap for ref, as MPSD does when the session changes.
func (f *fakeXbox) tap(ref mpsd.SessionReference) {
	f.mu.Lock()
	c := f.ws
	f.mu.Unlock()
	resource := ref.ServiceConfigID.String() + "~" + ref.TemplateName + "~" + ref.Name
	_ = wsjson.Write(context.Background(), c, []any{3, 1, map[string]any{"shoulderTaps": []map[string]any{{"resource": resource, "changeNumber": 2, "branch": uuid.NewString()}}}})
}

// published returns the last published custom properties.
func (f *fakeXbox) published() json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append(json.RawMessage(nil), f.custom...)
}

// newFakeXboxBroadcaster returns a started broadcaster whose primary session
// uses a real sessionNonceAnnouncer and go-xsapi clients against f.
func newFakeXboxBroadcaster(t *testing.T, f *fakeXbox) (*Broadcaster, *sessionNonceAnnouncer) {
	t.Helper()
	log := testBroadcasterLogger()
	conn, err := rta.Dial(t.Context(), f.httpClient(), log)
	if err != nil {
		t.Fatalf("dial rta: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := mpsd.New(f.httpClient(), conn, xsts.UserInfo{XUID: "100"}, log)
	nonce := newSessionNonceAnnouncer(&room.XBLAnnouncer{
		Client:           client,
		SessionReference: mpsd.SessionReference{ServiceConfigID: serviceConfigUUID, TemplateName: TemplateName, Name: strings.ToUpper(uuid.NewString())},
	}, "100", log)
	b := &Broadcaster{
		log:     log,
		started: true,
		conf: Config{
			XUID:   "100",
			Server: ServerInfo{Host: "127.0.0.1", Port: 19132},
			Status: Status{HostName: "Host", WorldName: "World", MaxPlayers: 20},
		},
	}
	b.ctx, b.cancel = context.WithCancel(t.Context())
	t.Cleanup(b.cancel)
	b.announcer = signalingConnectionAnnouncer{
		Announcer:  loggingAnnouncer{Announcer: nonce, log: log},
		connection: p2p.Connection{Type: p2p.ConnectionTypeSignalingOverWebSocket, NetherNetID: "123"},
	}
	if err := b.Update(t.Context()); err != nil {
		t.Fatalf("publish session: %v", err)
	}
	return b, nonce
}

// statusNonces decodes the nonce map from published custom properties.
func statusNonces(t *testing.T, custom json.RawMessage) map[string]string {
	t.Helper()
	var status struct {
		Nonces map[string]string `json:"nonces"`
	}
	if err := json.Unmarshal(custom, &status); err != nil {
		t.Fatalf("decode custom properties %s: %v", custom, err)
	}
	return status.Nonces
}
