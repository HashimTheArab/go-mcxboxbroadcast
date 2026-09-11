package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/df-mc/go-xsapi/v2/rta"
	xblsocial "github.com/df-mc/go-xsapi/v2/social"
	"github.com/df-mc/go-xsapi/v2/xal/xsts"
)

// TestBroadcasterClosePreservesSharedSocialSubscriber exercises the real social
// client over RTA and verifies that broadcaster shutdown removes only its handler.
func TestBroadcasterClosePreservesSharedSocialSubscriber(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	log := testBroadcasterLogger()
	peers := make(chan *websocket.Conn, 1)
	subscribed := make(chan struct{}, 1)
	var unsubscribes atomic.Int32
	const subscriptionID = 41
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		peer, err := websocket.Accept(w, req, &websocket.AcceptOptions{Subprotocols: []string{"rta.xboxlive.com.V2"}})
		if err != nil {
			t.Errorf("accept local RTA websocket: %v", err)
			return
		}
		defer peer.CloseNow()
		select {
		case peers <- peer:
		case <-ctx.Done():
			return
		}
		for {
			var frame []json.RawMessage
			if err := wsjson.Read(ctx, peer, &frame); err != nil {
				return
			}
			if len(frame) != 3 {
				t.Errorf("unexpected RTA request: %s", frame)
				return
			}
			var operation, sequence uint32
			if err := json.Unmarshal(frame[0], &operation); err != nil {
				t.Errorf("decode RTA operation: %v", err)
				return
			}
			if err := json.Unmarshal(frame[1], &sequence); err != nil {
				t.Errorf("decode RTA sequence: %v", err)
				return
			}
			var reply []any
			switch operation {
			case 1:
				var resource string
				if err := json.Unmarshal(frame[2], &resource); err != nil || resource != "https://social.xboxlive.com/users/xuid(100)/friends" {
					t.Errorf("unexpected social resource %s: %v", frame[2], err)
					return
				}
				select {
				case subscribed <- struct{}{}:
				default:
				}
				reply = []any{1, sequence, 0, subscriptionID, map[string]any{}}
			case 2:
				unsubscribes.Add(1)
				reply = []any{2, sequence, 0}
			default:
				t.Errorf("unexpected RTA operation %d", operation)
				return
			}
			if err := wsjson.Write(ctx, peer, reply); err != nil {
				return
			}
		}
	}))
	defer func() {
		cancel()
		server.Close()
	}()
	localURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: broadcasterRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host != "rta.xboxlive.com" {
			return nil, fmt.Errorf("unexpected external request: %s", req.URL)
		}
		local := req.Clone(req.Context())
		local.URL.Scheme, local.URL.Host, local.Host = localURL.Scheme, localURL.Host, localURL.Host
		return transport.RoundTrip(local)
	})}
	conn, err := rta.Dial(ctx, client, log)
	if err != nil {
		t.Fatalf("dial local RTA websocket: %v", err)
	}
	defer conn.Close()
	var peer *websocket.Conn
	select {
	case peer = <-peers:
	case <-ctx.Done():
		t.Fatal("local RTA server did not accept the connection")
	}
	defer func() {
		// Stop the RTA reconnect loop before forcibly closing its local peer.
		_ = conn.Close()
		_ = peer.CloseNow()
	}()
	social := xblsocial.New(client, conn, xsts.UserInfo{XUID: "100"}, log)
	b := &Broadcaster{log: log, started: true, done: make(chan struct{})}
	b.ctx, b.cancel = context.WithCancel(ctx)
	defer b.cancel()
	close(b.done)
	trigger := b.subscribeSocial(social, log)
	select {
	case <-subscribed:
	case <-ctx.Done():
		t.Fatal("broadcaster did not subscribe to social RTA")
	}
	// The first wire subscribe holds the social client's registration lock.
	// Registering this handler waits for the broadcaster's registration to finish.
	other := &sharedSocialEventHandler{counts: make(chan int, 2)}
	if err := social.Subscribe(ctx, other); err != nil {
		t.Fatalf("register other shared-client handler: %v", err)
	}
	for _, count := range []int{1, 2} {
		if err := wsjson.Write(ctx, peer, []any{3, subscriptionID, map[string]any{
			"NotificationType": "IncomingFriendRequestCountChanged", "Count": count,
		}}); err != nil {
			t.Fatalf("send social event: %v", err)
		}
		select {
		case got := <-other.counts:
			if got != count {
				t.Fatalf("shared-client handler received count %d, want %d", got, count)
			}
		case <-ctx.Done():
			t.Fatal("shared-client handler did not receive the social event")
		}
		if count == 1 {
			select {
			case <-trigger:
			case <-ctx.Done():
				t.Fatal("social event did not reach the broadcaster")
			}
			closed := make(chan error, 1)
			go func() { closed <- b.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatalf("close broadcaster: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("broadcaster shutdown did not release its social handler")
			}
			if got := unsubscribes.Load(); got != 0 {
				t.Fatalf("broadcaster removed the shared RTA subscription: %d unsubscribe requests", got)
			}
		}
	}
	select {
	case <-trigger:
		t.Fatal("removed broadcaster handler received a later social event")
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal("timed out checking broadcaster handler removal")
	}
	if err := social.Unsubscribe(ctx, other); err != nil {
		t.Fatalf("remove final shared-client handler: %v", err)
	}
	if got := unsubscribes.Load(); got != 1 {
		t.Fatalf("final handler removal sent %d RTA unsubscribe requests, want 1", got)
	}
}

// sharedSocialEventHandler records friend-request events for an independent owner.
type sharedSocialEventHandler struct {
	xblsocial.NopSubscriptionHandler
	counts chan int
}

// HandleIncomingFriendRequestCountChange records each event without blocking RTA.
func (h *sharedSocialEventHandler) HandleIncomingFriendRequestCountChange(count int) {
	select {
	case h.counts <- count:
	default:
	}
}
