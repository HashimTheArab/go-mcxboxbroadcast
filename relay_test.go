package broadcaster

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const testJoinURL = "https://bedrock.test:19132"

func TestCompleteOfferSDPAppendsTrickledCandidates(t *testing.T) {
	got := completeOfferSDP("v=0\r\na=fingerprint:sha-256 AA\r\n\r\n", []string{
		"candidate:1 1 udp 1 10.0.0.1 1 typ host",
		" a=candidate:2 1 udp 1 10.0.0.2 2 typ host ",
		"",
	})
	want := "v=0\r\na=fingerprint:sha-256 AA\r\n" +
		"a=candidate:1 1 udp 1 10.0.0.1 1 typ host\r\n" +
		"a=candidate:2 1 udp 1 10.0.0.2 2 typ host\r\n"
	if got != want {
		t.Fatalf("complete SDP =\n%q\nwant\n%q", got, want)
	}
}

// The offer is relayed 500 ms after the last candidate, carrying every candidate, and the answer goes back to the sender.
func TestHTTPRelayForwardsAnswerAfterCandidatesSettle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		target := &fakeJoinEndpoint{answer: "v=0 answer"}
		history := &syncHistoryStore{}
		sig, r := startTestRelay(t, target, history, nil)

		sig.signal(nethernet.SignalTypeOffer, "v=0\r\n")
		time.Sleep(300 * time.Millisecond)
		sig.signal(nethernet.SignalTypeCandidate, "candidate:1 1 udp 1 10.0.0.1 1 typ host")
		time.Sleep(499 * time.Millisecond)
		synctest.Wait()
		if target.posts() != 0 {
			t.Fatal("offer relayed before candidates settled")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()

		post := target.lastPost()
		if post.path != "/v1/join/42" || post.at != 800*time.Millisecond {
			t.Fatalf("POST %s at %s, want /v1/join/42 at 800ms", post.path, post.at)
		}
		if post.body != "v=0\r\na=candidate:1 1 udp 1 10.0.0.1 1 typ host\r\n" {
			t.Fatalf("relayed SDP = %q", post.body)
		}
		if probe := target.probe(); probe.Get("id") != "42" || probe.Get("version") != "1.26.60" || probe.Get("protocol") != "2225" || probe.Get("platform") != "-1" {
			t.Fatalf("probe query = %v", probe)
		}
		sent := sig.sent()
		if len(sent) != 1 || sent[0].Type != nethernet.SignalTypeAnswer || sent[0].ConnectionID != 42 || sent[0].NetworkID != "client" || sent[0].Data != "v=0 answer" {
			t.Fatalf("signals sent = %+v, want the answer for connection 42", sent)
		}
		if _, ok := history.lastSeen["2535400000000001"]; !ok {
			t.Fatal("relayed join was not recorded")
		}
		r.close()
	})
}

// Candidates that keep arriving cannot hold the offer past 3 s.
func TestHTTPRelayCapsCandidateGathering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		target := &fakeJoinEndpoint{answer: "answer"}
		sig, r := startTestRelay(t, target, nil, nil)
		defer r.close()
		sig.signal(nethernet.SignalTypeOffer, "v=0")
		for range 10 {
			time.Sleep(400 * time.Millisecond)
			sig.signal(nethernet.SignalTypeCandidate, "candidate:x")
		}
		synctest.Wait()
		if post := target.lastPost(); post.at != offerGatherMax {
			t.Fatalf("offer relayed at %s, want %s", post.at, offerGatherMax)
		}
		if got := strings.Count(target.lastPost().body, "a=candidate:x"); got != 7 {
			t.Fatalf("relayed %d candidates, want the 7 received before the cap", got)
		}
	})
}

func TestHTTPRelayMapsFailuresToConnectErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		target *fakeJoinEndpoint
		code   string
	}{
		"probe fails":          {&fakeJoinEndpoint{probeStatus: http.StatusServiceUnavailable}, "1"},
		"join request fails":   {&fakeJoinEndpoint{postStatus: http.StatusInternalServerError}, "6"},
		"join returns a code":  {&fakeJoinEndpoint{answer: "14"}, "6"},
		"join returns nothing": {&fakeJoinEndpoint{}, "6"},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				history := &syncHistoryStore{}
				sig, r := startTestRelay(t, tc.target, history, nil)
				defer r.close()
				sig.signal(nethernet.SignalTypeOffer, "v=0")
				time.Sleep(offerGatherQuiet)
				synctest.Wait()
				sent := sig.sent()
				if len(sent) != 1 || sent[0].Type != nethernet.SignalTypeError || sent[0].Data != tc.code || sent[0].ConnectionID != 42 || sent[0].NetworkID != "client" {
					t.Fatalf("signals sent = %+v, want CONNECTERROR 42 %s", sent, tc.code)
				}
				if len(history.lastSeen) != 0 {
					t.Fatal("failed join was recorded")
				}
			})
		})
	}
}

func TestHTTPRelayDropsOfferTheClientAbandons(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		target := &fakeJoinEndpoint{answer: "answer"}
		sig, r := startTestRelay(t, target, nil, nil)
		defer r.close()
		sig.signal(nethernet.SignalTypeOffer, "v=0")
		sig.signal(nethernet.SignalTypeError, "2")
		if sig.signal(nethernet.SignalTypeCandidate, "candidate:late") {
			t.Fatal("candidate for an abandoned offer was accepted")
		}
		time.Sleep(offerGatherMax)
		synctest.Wait()
		if target.posts() != 0 || len(sig.sent()) != 0 {
			t.Fatal("abandoned offer was relayed")
		}
	})
}

// A join whose identity cannot be verified is still answered but not recorded.
func TestHTTPRelaySkipsHistoryForUnverifiedOffer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		history := &syncHistoryStore{}
		sig, r := startTestRelay(t, &fakeJoinEndpoint{answer: "answer"}, history, errors.New("bad token"))
		defer r.close()
		sig.signal(nethernet.SignalTypeOffer, "v=0")
		time.Sleep(offerGatherQuiet)
		synctest.Wait()
		if sent := sig.sent(); len(sent) != 1 || sent[0].Type != nethernet.SignalTypeAnswer {
			t.Fatalf("signals sent = %+v, want the answer", sent)
		}
		if len(history.lastSeen) != 0 {
			t.Fatal("unverified join was recorded")
		}
	})
}

func TestVerifyOfferIdentityChecksTokenAndFingerprintBinding(t *testing.T) {
	issuerKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier := oidc.NewVerifier("https://authorization.test/", &oidc.StaticKeySet{PublicKeys: []crypto.PublicKey{&issuerKey.PublicKey}},
		&oidc.Config{ClientID: "api://auth-minecraft-services/multiplayer", SupportedSigningAlgs: []string{"RS256"}})
	offer := func(signedFingerprint, sdpFingerprint string, tokenKey *rsa.PrivateKey) string {
		return testOffer(t, tokenKey, clientKey, signedFingerprint, sdpFingerprint)
	}

	id, err := verifyOfferIdentity(context.Background(), offer("sha-256 AA:BB", "sha-256 AA:BB", issuerKey), verifier)
	if err != nil {
		t.Fatal(err)
	}
	if id != (offerIdentity{XUID: "2535400000000001", Gamertag: "Player"}) {
		t.Fatalf("identity = %+v", id)
	}
	if _, err := verifyOfferIdentity(context.Background(), offer("sha-256 AA:BB", "sha-256 CC:DD", issuerKey), verifier); err == nil {
		t.Fatal("accepted an identity signed for other fingerprints")
	}
	otherIssuer, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifyOfferIdentity(context.Background(), offer("sha-256 AA:BB", "sha-256 AA:BB", otherIssuer), verifier); err == nil {
		t.Fatal("accepted a token from another issuer key")
	}
	if _, err := verifyOfferIdentity(context.Background(), "v=0\r\na=fingerprint:sha-256 AA:BB\r\n", verifier); err == nil {
		t.Fatal("accepted an offer without identity")
	}
}

// testOffer builds an offer whose identity token is signed by issuer and whose
// client key signs signedFingerprint, while the SDP carries sdpFingerprint.
func testOffer(t *testing.T, issuer *rsa.PrivateKey, client *ecdsa.PrivateKey, signedFingerprint, sdpFingerprint string) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(&client.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	tokenSigner, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: issuer}, nil)
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.Signed(tokenSigner).Claims(jwt.Claims{
		Issuer:   "https://authorization.test/",
		Subject:  "player",
		Audience: jwt.Audience{"api://auth-minecraft-services/multiplayer"},
		Expiry:   jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}).Claims(struct {
		XUID     string `json:"xid"`
		Gamertag string `json:"xname"`
		Key      string `json:"cpk"`
	}{"2535400000000001", "Player", base64.StdEncoding.EncodeToString(der)}).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	payload, err := canonicalFingerprints([]string{signedFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	fingerprintSigner, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.ES384, Key: client}, nil)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := fingerprintSigner.Sign(payload)
	if err != nil {
		t.Fatal(err)
	}
	detached, err := signature.DetachedCompactSerialize()
	if err != nil {
		t.Fatal(err)
	}
	assertion, _ := json.Marshal(map[string]string{"fingerprints": detached, "token": token})
	identity, _ := json.Marshal(map[string]any{
		"assertion": string(assertion),
		"idp":       map[string]string{"domain": "https://authorization.test", "protocol": "default"},
	})
	return "v=0\r\na=identity:" + base64.StdEncoding.EncodeToString(identity) + "\r\na=fingerprint:" + sdpFingerprint + "\r\n"
}

// startTestRelay relays offers from connection 42 of "client" to fake target endpoints.
func startTestRelay(t *testing.T, target *fakeJoinEndpoint, history HistoryRecorder, verifyErr error) (*relaySignaling, *httpRelay) {
	t.Helper()
	sig := &relaySignaling{}
	target.start = time.Now()
	r := startHTTPRelay(t.Context(), sig, &httpRelay{
		joinURL: testJoinURL,
		client: endpoint.ClientConfig{
			HTTPClient: &http.Client{Transport: target},
			Logger:     testBroadcasterLogger(),
			ClientInfo: endpoint.ClientInfo{Version: "1.26.60", Protocol: 2225, Platform: relayClientPlatform},
		},
		verify: func(context.Context, string) (offerIdentity, error) {
			return offerIdentity{XUID: "2535400000000001", Gamertag: "Player"}, verifyErr
		},
		history: history,
		log:     testBroadcasterLogger(),
	})
	return sig, r
}

// relaySignaling is Xbox signaling that records what the relay sends.
type relaySignaling struct {
	fakeSignaling
	mu       sync.Mutex
	notifier nethernet.Notifier
	out      []nethernet.Signal
}

func (s *relaySignaling) Notify(n nethernet.Notifier) func() {
	s.mu.Lock()
	s.notifier = n
	s.mu.Unlock()
	return func() {}
}

func (s *relaySignaling) Signal(_ context.Context, signal *nethernet.Signal) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.out = append(s.out, *signal)
	return nil
}

// signal delivers an incoming signal for connection 42 from "client".
func (s *relaySignaling) signal(typ, data string) bool {
	s.mu.Lock()
	n := s.notifier
	s.mu.Unlock()
	return n.NotifySignal(&nethernet.Signal{Type: typ, ConnectionID: 42, Data: data, NetworkID: "client"})
}

func (s *relaySignaling) sent() []nethernet.Signal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]nethernet.Signal(nil), s.out...)
}

// fakeJoinEndpoint serves the target's /v1/join probe and join requests.
type fakeJoinEndpoint struct {
	probeStatus, postStatus int
	answer                  string
	start                   time.Time

	mu      sync.Mutex
	query   url.Values
	history []joinPost
}

type joinPost struct {
	path, body string
	at         time.Duration
}

func (e *fakeJoinEndpoint) RoundTrip(req *http.Request) (*http.Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if req.URL.Host != "bedrock.test:19132" {
		return nil, errors.New("unexpected host " + req.URL.Host)
	}
	if req.Method == http.MethodGet {
		e.query = req.URL.Query()
		return response(defaultStatus(e.probeStatus), `{"name":"World","level":"Host","players":1,"maxPlayers":10}`), nil
	}
	body, _ := io.ReadAll(req.Body)
	e.history = append(e.history, joinPost{path: req.URL.Path, body: string(body), at: time.Since(e.start)})
	return response(defaultStatus(e.postStatus), e.answer), nil
}

func defaultStatus(code int) int {
	if code == 0 {
		return http.StatusOK
	}
	return code
}

func (e *fakeJoinEndpoint) posts() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.history)
}

func (e *fakeJoinEndpoint) lastPost() joinPost {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.history) == 0 {
		return joinPost{}
	}
	return e.history[len(e.history)-1]
}

func (e *fakeJoinEndpoint) probe() url.Values {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.query
}
