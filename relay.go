package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/pion/sdp/v3"
	"github.com/sandertv/gophertunnel/minecraft/service"
	"golang.org/x/oauth2"
)

const (
	offerGatherQuiet   = 500 * time.Millisecond // wait after the last trickled candidate
	offerGatherMax     = 3 * time.Second        // longest wait for candidates
	relayTimeout       = 15 * time.Second       // bounds the probe and join request
	relayReplyTimeout  = 5 * time.Second        // bounds sending the answer or error, after relayTimeout
	relayRecordTimeout = 5 * time.Second        // bounds recording the join, after the reply
	// relayClientPlatform is the platform the relay reports on probes, as the Java relay does.
	relayClientPlatform = -1
)

// Connect error codes the client shows as session-not-found and connect-request failures.
const (
	relayErrorServerNotFound = nethernet.ErrorCodeDestinationNotLoggedIn
	relayErrorConnectRequest = nethernet.ErrorCodeConnectRequest
)

// offerIdentity is the verified player behind a join offer.
type offerIdentity struct {
	XUID     string
	Gamertag string
}

// httpRelay forwards join offers from Xbox signaling to the target's HTTP
// signaling endpoint and returns its answer, so the client connects to the
// target directly.
type httpRelay struct {
	signaling nethernet.Signaling
	joinURL   string
	client    endpoint.ClientConfig // NetworkID is set per offer
	verify    func(ctx context.Context, sdp string) (offerIdentity, error)
	history   HistoryRecorder // nil skips history
	log       *slog.Logger
	ctx       context.Context
	cancel    context.CancelFunc

	quiet, max time.Duration

	mu      sync.Mutex
	pending map[offerKey]*pendingOffer
	closed  bool
	stop    func()
	wg      sync.WaitGroup
}

type offerKey struct {
	networkID    string
	connectionID uint64
}

// pendingOffer collects the candidates trickled after one offer.
type pendingOffer struct {
	key        offerKey
	sdp        string
	candidates []string
	quiet, max *time.Timer
}

func (o *pendingOffer) cancel() {
	o.quiet.Stop()
	o.max.Stop()
}

// startHTTPRelay subscribes to sig and relays its offers until close.
func startHTTPRelay(ctx context.Context, sig nethernet.Signaling, r *httpRelay) *httpRelay {
	r.signaling = sig
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.pending = make(map[offerKey]*pendingOffer)
	if r.quiet == 0 {
		r.quiet = offerGatherQuiet
	}
	if r.max == 0 {
		r.max = offerGatherMax
	}
	r.stop = sig.Notify(r)
	return r
}

// close stops accepting signals and waits for in-flight relays.
func (r *httpRelay) close() {
	r.stop()
	r.mu.Lock()
	r.closed = true
	for key, o := range r.pending {
		o.cancel()
		delete(r.pending, key)
	}
	r.mu.Unlock()
	r.cancel()
	r.wg.Wait()
}

// NotifySignal implements nethernet.Notifier.
func (r *httpRelay) NotifySignal(signal *nethernet.Signal) bool {
	key := offerKey{networkID: signal.NetworkID, connectionID: signal.ConnectionID}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	switch signal.Type {
	case nethernet.SignalTypeOffer:
		if previous := r.pending[key]; previous != nil {
			previous.cancel()
		}
		o := &pendingOffer{key: key, sdp: signal.Data}
		o.max = time.AfterFunc(r.max, func() { r.flush(o) })
		o.quiet = time.AfterFunc(r.quiet, func() { r.flush(o) })
		r.pending[key] = o
		r.log.Debug("received join offer", "sender", key.networkID, "connection_id", key.connectionID)
		return true
	case nethernet.SignalTypeCandidate:
		o := r.pending[key]
		if o == nil {
			r.log.Debug("dropping candidate for a relayed or unknown offer", "sender", key.networkID, "connection_id", key.connectionID)
			return false
		}
		o.candidates = append(o.candidates, signal.Data)
		o.quiet.Reset(r.quiet)
		return true
	case nethernet.SignalTypeError:
		if o := r.pending[key]; o != nil {
			o.cancel()
			delete(r.pending, key)
		}
		r.log.Debug("client reported connect error", "sender", key.networkID, "connection_id", key.connectionID, "code", signal.Data)
		return true
	default:
		r.log.Debug("unhandled signal", "sender", key.networkID, "type", signal.Type)
		return false
	}
}

// flush relays o once, whichever of its timers fires first.
func (r *httpRelay) flush(o *pendingOffer) {
	r.mu.Lock()
	if r.closed || r.pending[o.key] != o {
		r.mu.Unlock()
		return
	}
	delete(r.pending, o.key)
	o.cancel()
	sdp := completeOfferSDP(o.sdp, o.candidates)
	r.wg.Add(1)
	r.mu.Unlock()
	defer r.wg.Done()
	r.relay(o, sdp)
}

func (r *httpRelay) relay(o *pendingOffer, sdp string) {
	ctx, cancel := context.WithTimeout(r.ctx, relayTimeout)
	defer cancel()
	r.log.Debug("relaying join offer", "sender", o.key.networkID, "connection_id", o.key.connectionID, "candidates", len(o.candidates), "target", r.joinURL)

	conf := r.client
	conf.NetworkID = strconv.FormatUint(o.key.connectionID, 10)
	client := conf.New()
	if _, err := client.Status(ctx, r.joinURL); err != nil {
		r.fail(o.key, relayErrorServerNotFound, err)
		return
	}
	answers := make(chan string, 1)
	defer client.Notify(answerNotifier(answers))()
	err := client.Signal(ctx, &nethernet.Signal{
		Type:         nethernet.SignalTypeOffer,
		ConnectionID: o.key.connectionID,
		Data:         sdp,
		NetworkID:    r.joinURL,
	})
	if err != nil {
		r.fail(o.key, relayErrorConnectRequest, err)
		return
	}
	var answer string
	select {
	case answer = <-answers:
	default:
		r.fail(o.key, relayErrorConnectRequest, errors.New("join request returned no answer"))
		return
	}
	if err := r.reply(&nethernet.Signal{
		Type:         nethernet.SignalTypeAnswer,
		ConnectionID: o.key.connectionID,
		Data:         answer,
		NetworkID:    o.key.networkID,
	}); err != nil {
		r.log.Error("send join answer", "sender", o.key.networkID, "err", err)
		return
	}
	r.recordJoin(o)
}

// fail tells the client why its join failed.
func (r *httpRelay) fail(key offerKey, code int, cause error) {
	r.log.Error("relay join offer", "sender", key.networkID, "target", r.joinURL, "code", code, "err", cause)
	if err := r.reply(&nethernet.Signal{
		Type:         nethernet.SignalTypeError,
		ConnectionID: key.connectionID,
		Data:         strconv.Itoa(code),
		NetworkID:    key.networkID,
	}); err != nil {
		r.log.Debug("send connect error", "sender", key.networkID, "err", err)
	}
}

// reply sends signal to the client with its own deadline, since the relay's
// may have expired waiting on the target.
func (r *httpRelay) reply(signal *nethernet.Signal) error {
	ctx, cancel := context.WithTimeout(r.ctx, relayReplyTimeout)
	defer cancel()
	return r.signaling.Signal(ctx, signal)
}

// recordJoin marks the offer's verified player as seen, with its own deadline
// since the relay's may be nearly spent.
func (r *httpRelay) recordJoin(o *pendingOffer) {
	ctx, cancel := context.WithTimeout(r.ctx, relayRecordTimeout)
	defer cancel()
	id, err := r.verify(ctx, o.sdp)
	if err != nil {
		r.log.Debug("unable to record join", "sender", o.key.networkID, "err", err)
		return
	}
	r.log.Info("relayed bedrock client", "xuid", id.XUID, "name", id.Gamertag, "target", r.joinURL)
	if r.history == nil {
		return
	}
	if err := r.history.Seen(ctx, id.XUID, time.Now()); err != nil {
		r.log.Error("record player history", "xuid", id.XUID, "err", err)
	}
}

type answerNotifier chan<- string

func (n answerNotifier) NotifySignal(signal *nethernet.Signal) bool {
	if signal.Type != nethernet.SignalTypeAnswer {
		return false
	}
	select {
	case n <- signal.Data:
	default:
	}
	return true
}

// completeOfferSDP appends trickled candidates to the offer, since HTTP
// signaling needs every candidate up front.
func completeOfferSDP(sdp string, candidates []string) string {
	var b strings.Builder
	b.WriteString(strings.TrimRight(sdp, " \t\r\n"))
	b.WriteString("\r\n")
	for _, candidate := range candidates {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if !strings.HasPrefix(candidate, "a=") {
			b.WriteString("a=")
		}
		b.WriteString(candidate)
		b.WriteString("\r\n")
	}
	return b.String()
}

// tokenVerifier lazily resolves the verifier for Minecraft multiplayer tokens.
type tokenVerifier struct {
	httpClient *http.Client

	mu       sync.Mutex
	verifier *oidc.IDTokenVerifier
}

func (v *tokenVerifier) get(ctx context.Context) (*oidc.IDTokenVerifier, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.verifier != nil {
		return v.verifier, nil
	}
	if v.httpClient != nil {
		ctx = context.WithValue(ctx, oauth2.HTTPClient, v.httpClient)
	}
	discovery, err := service.Default(ctx)
	if err != nil {
		return nil, fmt.Errorf("discover service endpoints: %w", err)
	}
	env := &service.AuthorizationEnvironment{HTTPClient: v.httpClient}
	if err := discovery.Environment(env); err != nil {
		return nil, fmt.Errorf("decode authorization environment: %w", err)
	}
	verifier, err := env.VerifierContext(ctx)
	if err != nil {
		return nil, err
	}
	v.verifier = verifier
	return verifier, nil
}

// verifyOfferIdentity checks that the offer's identity token was issued by
// Minecraft's authorization service and signed the offer's DTLS fingerprints.
func verifyOfferIdentity(ctx context.Context, offer string, verifier *oidc.IDTokenVerifier) (offerIdentity, error) {
	d := &sdp.SessionDescription{}
	if err := d.UnmarshalString(offer); err != nil {
		return offerIdentity{}, fmt.Errorf("decode offer: %w", err)
	}
	desc, err := nethernet.ParseDescription(d)
	if err != nil {
		return offerIdentity{}, fmt.Errorf("parse offer: %w", err)
	}
	if desc.Identity == nil {
		return offerIdentity{}, errors.New("offer has no identity")
	}
	token, err := verifier.Verify(ctx, desc.Identity.Assertion.Token)
	if err != nil {
		return offerIdentity{}, fmt.Errorf("verify token: %w", err)
	}
	var claims struct {
		XUID     string `json:"xid"`
		Gamertag string `json:"xname"`
	}
	if err := token.Claims(&claims); err != nil {
		return offerIdentity{}, fmt.Errorf("decode token claims: %w", err)
	}
	if claims.XUID == "" {
		return offerIdentity{}, errors.New("token has no xuid")
	}
	key, err := desc.Identity.PublicKey(false)
	if err != nil {
		return offerIdentity{}, fmt.Errorf("token key: %w", err)
	}
	if err := desc.Identity.Verify(desc, key); err != nil {
		return offerIdentity{}, err
	}
	return offerIdentity{XUID: claims.XUID, Gamertag: claims.Gamertag}, nil
}
