package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service"
	"github.com/sandertv/gophertunnel/minecraft/service/signaling"
)

const defaultSignalingDialTimeout = 15 * time.Second

// Broadcaster owns the Xbox Live sessions and relays their join offers to the
// target server's HTTP signaling.
type Broadcaster struct {
	conf Config
	log  *slog.Logger

	announcer         room.Announcer
	retiredSessions   []retiredSession // rotated-out sessions, closed once drained
	relay             *httpRelay
	tokens            *tokenVerifier
	signaling         nethernet.Signaling
	sessionConnection *p2p.Connection
	staleAnnouncers   []room.Announcer // sessions whose close failed, retried on Update
	announcerFactory  func(*Broadcaster) room.Announcer

	ctx    context.Context
	cancel context.CancelFunc

	mu              sync.Mutex
	galleryMu       sync.Mutex          // serializes uploads, since recovery starts one too
	tokensMu        sync.Mutex          // guards minecraftTokens, which gallery uploads share with signaling dials
	minecraftTokens service.TokenSource // created on first use when conf.MinecraftTokenSource is nil
	workers         sync.WaitGroup      // goroutines using XBLClient, which the caller closes after Close
	started         bool
	failure         error
	recovering      bool
	socialWg        sync.WaitGroup // social subscriptions not yet released
	// healthCheck asks the session loop for an early health check, such as
	// when a session nears its member cap.
	healthCheck chan struct{}

	health healthState

	// lastQuery is the most recent successful target-server query, kept so
	// query failures fall back to real data instead of failing the update.
	lastQuery *minecraft.ServerStatus
}

// New validates conf and returns a Broadcaster.
func New(conf Config) (*Broadcaster, error) {
	if err := validateJoinURL(conf.JoinURL); err != nil {
		return nil, err
	}
	if err := conf.Status.validate(); err != nil {
		return nil, err
	}
	if conf.XBLClient == nil {
		return nil, errors.New("xbox live client is required")
	}
	if conf.Log == nil {
		conf.Log = slog.Default()
	}
	if conf.UpdateInterval == 0 {
		conf.UpdateInterval = defaultUpdateInterval
	}
	conf.UpdateInterval = max(conf.UpdateInterval, minUpdateInterval)
	return &Broadcaster{
		conf:        conf,
		log:         conf.Log.With("src", "broadcaster"),
		tokens:      &tokenVerifier{httpClient: conf.HTTPClient},
		healthCheck: make(chan struct{}, 1),
	}, nil
}

// Start publishes the Xbox session and starts relaying join offers.
func (b *Broadcaster) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.started {
		return errors.New("broadcaster already started")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	b.ctx, b.cancel = context.WithCancel(ctx)
	b.failure = nil
	b.log.Debug("starting broadcaster",
		"status_provider", b.conf.StatusProvider != nil,
		"friend_sync", b.conf.FriendSync != nil,
		"gallery", b.conf.Gallery != nil,
	)
	fail := func(err error) error {
		b.cancel()
		return errors.Join(err, b.releaseSessions())
	}
	sig, err := b.signalingFor(b.ctx)
	if err != nil {
		return fail(err)
	}
	b.signaling = sig
	b.log.Debug("nethernet signaling ready", "signaling_type", fmt.Sprintf("%T", sig), "network_id", sig.NetworkID())
	b.log.Info("creating xbox live session")
	if err := b.publishOn(b.ctx, sig); err != nil {
		return fail(err)
	}
	b.log.Info("created xbox live session")
	b.started = true
	b.health.setIdentity(b.xuid(), b.conf.XBLClient.UserInfo().GamerTag)
	b.log.Info("nethernet broadcaster started", "network_id", sig.NetworkID(), "target", b.conf.JoinURL)

	b.spawn(b.sessionLoop)
	b.spawn(func() { runPresence(b.ctx, b.conf.XBLClient.Presence(), b.log) })
	b.spawn(b.uploadGallery)
	if b.conf.FriendSync != nil {
		b.startFriendSync()
	}
	b.spawn(b.logSocialSummary)
	return nil
}

// logSocialSummary logs the account and its friend count. The social summary's
// targetFollowingCount is unreliable for the caller's own profile, so the count comes from the
// friend list, like MCXboxBroadcast.
func (b *Broadcaster) logSocialSummary() {
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	defer cancel()
	friends, err := b.friendClient().Friends(ctx)
	if err != nil {
		b.log.Debug("fetch friend list for summary", "err", err)
		return
	}
	b.log.Info("authenticated to xbox live",
		"gamertag", b.hostNameFallback(),
		"xuid", b.xuid(),
		"friends", len(friends),
	)
}

// startFriendSync runs friend sync until the broadcaster stops. Only accepting requests acts on
// social events, so the RTA feed is subscribed only with AutoFriend.
func (b *Broadcaster) startFriendSync() {
	conf := b.conf.FriendSync
	b.log.Debug("starting friend sync",
		"auto_friend", conf.AutoFriend,
		"initial_invite", conf.InitialInvite,
		"expiry_enabled", conf.ExpiryEnabled,
	)
	syncer := FriendSyncer{
		Client:   b.friendClient(),
		Config:   *conf,
		History:  b.conf.FriendHistory,
		Notifier: b.conf.Notifier,
		Log:      b.log,

		friendCount: &b.health.friends,
	}
	if conf.InitialInvite {
		syncer.Inviter = b
	}
	if conf.AutoFriend {
		syncer.Trigger = b.subscribeSocial(b.conf.XBLClient.Social())
	}
	b.spawn(func() { syncer.Run(b.ctx) })
}

// startRelay relays offers received on sig until the next recreation or Close.
func (b *Broadcaster) startRelay(sig nethernet.Signaling) *httpRelay {
	recorder, _ := b.conf.FriendHistory.(HistoryRecorder)
	return startHTTPRelay(b.ctx, sig, &httpRelay{
		joinURL: b.conf.JoinURL,
		client: endpoint.ClientConfig{
			HTTPClient: b.conf.HTTPClient,
			Logger:     b.log,
			ClientInfo: b.clientInfo(),
		},
		verify: func(ctx context.Context, sdp string) (offerIdentity, error) {
			verifier, err := b.tokens.get(ctx)
			if err != nil {
				return offerIdentity{}, err
			}
			return verifyOfferIdentity(ctx, sdp, verifier)
		},
		history: recorder,
		joins:   &b.health.joins,
		log:     b.log,
	})
}

// minecraftTokenSource returns the shared Minecraft token source, creating it from XBLClient on first use.
func (b *Broadcaster) minecraftTokenSource(ctx context.Context) (service.TokenSource, error) {
	if tokens := b.cachedMinecraftTokens(); tokens != nil {
		return tokens, nil
	}
	tokens, err := NewMinecraftTokenSource(ctx, b.conf.XBLClient, b.conf.HTTPClient, b.log)
	if err != nil {
		return nil, err
	}
	return b.storeMinecraftTokens(tokens), nil
}

func (b *Broadcaster) cachedMinecraftTokens() service.TokenSource {
	if b.conf.MinecraftTokenSource != nil {
		return b.conf.MinecraftTokenSource
	}
	b.tokensMu.Lock()
	defer b.tokensMu.Unlock()
	return b.minecraftTokens
}

// storeMinecraftTokens keeps the first token source created and returns the one to use.
func (b *Broadcaster) storeMinecraftTokens(tokens service.TokenSource) service.TokenSource {
	if b.conf.MinecraftTokenSource != nil {
		return b.conf.MinecraftTokenSource
	}
	b.tokensMu.Lock()
	defer b.tokensMu.Unlock()
	if b.minecraftTokens == nil {
		b.minecraftTokens = tokens
	}
	return b.minecraftTokens
}

func (b *Broadcaster) xuid() string {
	return b.conf.XBLClient.UserInfo().XUID
}

func (b *Broadcaster) friendClient() FriendClient {
	return FriendClient{Social: b.conf.XBLClient.Social()}
}

// Invite sends a Minecraft game invite to xuid through the current MPSD session.
func (b *Broadcaster) Invite(ctx context.Context, xuid string) error {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return errors.New("invite: broadcaster not started")
	}
	announcer, ok := xblAnnouncer(b.announcer)
	if !ok {
		b.mu.Unlock()
		return errors.New("invite: no active MPSD session")
	}
	announcer.Lock()
	session := announcer.Session
	announcer.Unlock()
	b.mu.Unlock()
	if session == nil || session.Context().Err() != nil {
		return errors.New("invite: no active MPSD session")
	}
	_, err := session.Invite(ctx, xuid, strconv.FormatInt(TitleID, 10))
	return err
}

// signalingFor returns or creates the nethernet signaling connection.
func (b *Broadcaster) signalingFor(ctx context.Context) (nethernet.Signaling, error) {
	if b.conf.Signaling != nil {
		b.log.Debug("using configured nethernet signaling", "signaling_type", fmt.Sprintf("%T", b.conf.Signaling))
		return b.conf.Signaling, nil
	}
	if b.conf.SignalingFactory != nil {
		b.log.Debug("creating nethernet signaling from factory")
		conf := b.conf
		conf.MinecraftTokenSource = b.cachedMinecraftTokens()
		sig, err := b.conf.SignalingFactory(ctx, conf)
		if err != nil {
			return nil, err
		}
		if sig == nil {
			return nil, errors.New("signaling factory returned nil signaling")
		}
		b.log.Debug("created nethernet signaling from factory", "signaling_type", fmt.Sprintf("%T", sig), "network_id", sig.NetworkID())
		return sig, nil
	}
	return b.dialSignaling(ctx)
}

// dialSignaling dials Xbox JSON-RPC signaling, first creating the Minecraft token source if
// needed. The dial runs apart so a step that ignores its context cannot hold Start past the timeout.
func (b *Broadcaster) dialSignaling(ctx context.Context) (nethernet.Signaling, error) {
	timeout := b.signalingDialTimeout()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	b.log.Debug("dialing nethernet signaling", "timeout", timeout)
	httpClient := signalingStartupHTTPClient(b.conf.HTTPClient, timeout)
	xbl, tokens, log := b.conf.XBLClient, b.cachedMinecraftTokens(), b.log
	type dialed struct {
		sig    nethernet.Signaling
		tokens service.TokenSource
		err    error
	}
	results := make(chan dialed)
	b.spawn(func() {
		r := dialed{tokens: tokens}
		if r.tokens == nil {
			// Kept even if the dial fails, so recovery attempts do not log in to PlayFab again.
			if r.tokens, r.err = NewMinecraftTokenSource(ctx, xbl, httpClient, log); r.err == nil {
				r.tokens = b.storeMinecraftTokens(r.tokens)
			}
		}
		if r.err == nil {
			// Keep r.sig an untyped nil on failure so closeSignaling skips it.
			sig, err := signaling.Dialer{Log: log, HTTPClient: httpClient}.DialContext(ctx, r.tokens)
			if r.err = err; err == nil {
				r.sig = sig
			}
		}
		select {
		case results <- r:
		case <-ctx.Done():
			_ = closeSignaling(r.sig)
		}
	})
	select {
	case r := <-results:
		if ctx.Err() != nil {
			_ = closeSignaling(r.sig)
			return nil, fmt.Errorf("dial nethernet signaling: %w", ctx.Err())
		}
		if r.err != nil {
			return nil, r.err
		}
		return r.sig, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("dial nethernet signaling: %w", ctx.Err())
	}
}

// signalingDialTimeout returns the configured or default signaling dial timeout.
func (b *Broadcaster) signalingDialTimeout() time.Duration {
	if b.conf.SignalingDialTimeout > 0 {
		return b.conf.SignalingDialTimeout
	}
	return defaultSignalingDialTimeout
}

// signalingStartupHTTPClient returns a clone of the HTTP client with a startup timeout applied.
func signalingStartupHTTPClient(client *http.Client, timeout time.Duration) *http.Client {
	if timeout <= 0 {
		return client
	}
	if client == nil {
		return &http.Client{Timeout: timeout}
	}
	if client.Timeout > 0 && client.Timeout <= timeout {
		return client
	}
	clone := *client
	clone.Timeout = timeout
	return &clone
}

// closeSignaling closes sig if its implementation can be closed; nethernet.Signaling has no Close.
func closeSignaling(sig nethernet.Signaling) error {
	if c, ok := sig.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// publishOn publishes a new session that advertises sig, then relays the offers sig receives.
func (b *Broadcaster) publishOn(ctx context.Context, sig nethernet.Signaling) error {
	connection, err := b.signalingConnection(sig)
	if err != nil {
		return err
	}
	b.sessionConnection = connection
	b.log.Debug("using session signaling connection", "connection_type", connection.Type, "nethernet_id", connection.NetherNetID, "pmsg_id", connection.PlayerMessagingID)
	announcer, err := b.publishSession(ctx, b.conf.SessionName)
	if err != nil {
		return err
	}
	b.announcer = announcer
	b.relay = b.startRelay(sig)
	return nil
}

// publishSession announces a new session on the current signaling connection and closes it
// again if the announcement fails.
func (b *Broadcaster) publishSession(ctx context.Context, name string) (room.Announcer, error) {
	announcer := b.wrapAnnouncer(b.newAnnouncer(name))
	if err := announcer.Announce(ctx, b.status(ctx)); err != nil {
		return nil, errors.Join(fmt.Errorf("announce session: %w", err), announcer.Close())
	}
	return announcer, nil
}

// Update refreshes the announced Xbox session metadata.
func (b *Broadcaster) Update(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started {
		return errors.New("broadcaster not started")
	}
	if b.recovering {
		return errors.New("session recovery is in progress")
	}
	if b.announcer == nil {
		return errors.New("no published session")
	}
	if err := b.announcer.Announce(ctx, b.status(ctx)); err != nil {
		return err
	}
	b.closeStaleSessions()
	// Matching MCXboxBroadcast, suppressSessionUpdateMessage only demotes the
	// periodic success log to debug level.
	if b.conf.SuppressSessionUpdateMessage {
		b.log.Debug("updated session")
	} else {
		b.log.Info("updated session")
	}
	return nil
}

// closeStaleSessions retries closing sessions whose first close attempt
// failed. The caller must hold b.mu.
func (b *Broadcaster) closeStaleSessions() {
	if len(b.staleAnnouncers) == 0 {
		return
	}
	remaining := b.staleAnnouncers[:0]
	for _, announcer := range b.staleAnnouncers {
		if err := announcer.Close(); err != nil {
			b.log.Warn("retry close stale session", "err", err)
			remaining = append(remaining, announcer)
		}
	}
	b.staleAnnouncers = remaining
}

// cleanupPublishedSessions closes the current, retired and stale sessions.
func (b *Broadcaster) cleanupPublishedSessions() error {
	var err error
	var failed []room.Announcer
	stale := b.staleAnnouncers
	for _, retired := range b.retiredSessions {
		stale = append(stale, retired.announcer)
	}
	for _, stale := range stale {
		if closeErr := stale.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
			failed = append(failed, stale)
		}
	}
	b.retiredSessions = nil
	b.staleAnnouncers = failed
	if b.announcer != nil {
		if closeErr := b.announcer.Close(); closeErr != nil {
			// A failed close leaves the session advertised; keep it so a later Update retries.
			err = errors.Join(err, closeErr)
			b.staleAnnouncers = append(b.staleAnnouncers, b.announcer)
		}
		b.announcer = nil
	}
	b.sessionConnection = nil
	return err
}

// releaseSessions stops the relay and closes every session and the signaling connection.
func (b *Broadcaster) releaseSessions() error {
	if b.relay != nil {
		b.relay.close()
		b.relay = nil
	}
	err := errors.Join(b.cleanupPublishedSessions(), closeSignaling(b.signaling))
	b.signaling = nil
	return err
}

// Close stops the relay and removes the Xbox sessions.
func (b *Broadcaster) Close() error {
	b.mu.Lock()
	if !b.started {
		b.mu.Unlock()
		return nil
	}
	b.cancel()
	// The caller may close XBLClient right after; its social handlers must be released first.
	b.socialWg.Wait()
	err := b.releaseSessions()
	b.started = false
	b.mu.Unlock()
	// Workers may need b.mu to notice cancellation, so wait after releasing it. The wait is
	// bounded so a call that ignores cancellation cannot stop the process from exiting.
	done := make(chan struct{})
	go func() {
		b.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(closeWorkersTimeout):
		b.log.Warn("closed while background work was still running", "waited", closeWorkersTimeout)
	}
	return err
}

// closeWorkersTimeout outlasts every bounded worker call, the longest being a gallery upload.
const closeWorkersTimeout = galleryUploadTimeout + 5*time.Second

// spawn runs fn in a goroutine that Close waits for.
func (b *Broadcaster) spawn(fn func()) {
	b.workers.Add(1)
	go func() {
		defer b.workers.Done()
		fn()
	}()
}

// Wait blocks until shutdown is requested and returns any terminal recovery error.
// Call Close afterward to release the relay and Xbox sessions.
func (b *Broadcaster) Wait() error {
	b.mu.Lock()
	ctx := b.ctx
	b.mu.Unlock()
	if ctx == nil {
		return nil
	}
	<-ctx.Done()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failure
}
