package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
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

	announcer                  room.Announcer
	retiredSessions            []retiredSession // rotated-out sessions, closed once drained
	relay                      *httpRelay
	tokens                     *tokenVerifier
	signaling                  nethernet.Signaling
	sessionRef                 mpsd.SessionReference
	sessionConnection          *p2p.Connection
	subAnnouncers              []publishedSubAccount
	subAnnouncersByID          map[string]room.Announcer
	staleSubAnnouncers         []room.Announcer
	announcerFactory           func(*Broadcaster) room.Announcer
	subAccountAnnouncerFactory func(context.Context, SubAccountConfig, mpsd.SessionReference) (room.Announcer, error)
	xblClient                  *xsapi.Client
	minecraftTokens            service.TokenSource
	subMinecraftTokens         map[*xsapi.Client]service.TokenSource
	createdXBLClients          []*xsapi.Client

	ctx    context.Context
	cancel context.CancelFunc

	mu         sync.Mutex
	galleryMu  sync.Mutex
	started    bool
	failure    error
	recovering bool
	// socialWg tracks social subscription cleanup before clients are closed.
	socialWg sync.WaitGroup
	// healthCheck asks the session loop for an early health check, such as
	// when a session nears its member cap.
	healthCheck chan struct{}

	// lastQuery is the most recent successful target-server query, kept so
	// query failures fall back to real data instead of failing the update.
	lastQuery *minecraft.ServerStatus

	// rotationGrace overrides sessionRotateGrace in tests.
	rotationGrace time.Duration
	// subAccountStartTimeout bounds each sub-account's session publish so a hung
	// directory or RTA call degrades to a skipped sub-account instead of
	// blocking broadcaster startup. Zero uses the default.
	subAccountStartTimeout time.Duration
	// subAccountSettleDelay is the wait after establishing a new sub-account
	// friendship before joining the session.
	subAccountSettleDelay time.Duration
	// galleryUploadTimeout bounds each account's gallery upload. Zero uses the
	// default.
	galleryUploadTimeout time.Duration
}

type publishedSubAccount struct {
	id        string
	xuid      string
	announcer room.Announcer
}

// defaultSignalingResult holds the result of a default signaling dial.
type defaultSignalingResult struct {
	signaling     nethernet.Signaling
	minecraft     service.TokenSource
	createdClient *xsapi.Client
	err           error
}

// defaultSignalingConfig holds the inputs for creating a default signaling connection.
type defaultSignalingConfig struct {
	log             *slog.Logger
	httpClient      *http.Client
	xblClient       *xsapi.Client
	xblTokenSource  xsapi.TokenSource
	minecraftTokens service.TokenSource
}

// New validates conf and returns a Broadcaster.
func New(conf Config) (*Broadcaster, error) {
	if err := validateJoinURL(conf.JoinURL); err != nil {
		return nil, err
	}
	if err := conf.Status.validate(); err != nil {
		return nil, err
	}
	for _, account := range conf.SubAccounts {
		if account.Enabled {
			return nil, errors.New("sub-accounts are not supported: jsonrpc signaling cannot share its player messaging identity across their sessions")
		}
	}
	if conf.XBLClient == nil && conf.XBLTokenSource == nil {
		return nil, errors.New("xbox live client or token source is required")
	}
	if conf.Log == nil {
		conf.Log = slog.Default()
	}
	if conf.UpdateInterval == 0 {
		conf.UpdateInterval = 30 * time.Second
	}
	if conf.UpdateInterval < 20*time.Second {
		conf.UpdateInterval = 20 * time.Second
	}
	return &Broadcaster{
		conf:                  conf,
		log:                   conf.Log.With("src", "broadcaster"),
		tokens:                &tokenVerifier{httpClient: conf.HTTPClient},
		healthCheck:           make(chan struct{}, 1),
		subAccountSettleDelay: 5 * time.Second,
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
	b.debug("starting broadcaster",
		"status_provider", b.conf.StatusProvider != nil,
		"sub_accounts", len(b.conf.SubAccounts),
		"friend_sync", b.conf.FriendSync != nil,
		"gallery", b.conf.Gallery != nil && b.conf.Gallery.Enabled,
	)
	sig, err := b.signalingFor(b.ctx)
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(false))
	}
	b.signaling = sig
	b.debug("nethernet signaling ready", "signaling_type", fmt.Sprintf("%T", sig), "network_id", signalingNetworkID(sig))

	status, err := b.status(b.ctx)
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(false))
	}
	b.debugRoomStatus("resolved room status", status)
	b.announcer, err = b.newAnnouncer(b.ctx, b.conf.SessionName)
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(false))
	}
	connection, err := b.signalingConnection(sig)
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(true))
	}
	b.sessionConnection = connection
	b.announcer = b.wrapAnnouncer(b.announcer, b.log)
	b.debug("using session signaling connection", "connection_type", connection.Type, "nethernet_id", connection.NetherNetID, "pmsg_id", connection.PlayerMessagingID)
	b.info("creating xbox live session")
	if err := b.announcer.Announce(b.ctx, status); err != nil {
		b.cancel()
		err = errors.Join(fmt.Errorf("announce session: %w", err), b.cleanupStartupFailure(true))
		return err
	}
	b.info("created xbox live session")
	b.debug("starting sub-account sessions", "count", len(b.conf.SubAccounts))
	if err := b.startSubAccounts(b.ctx, status); err != nil {
		b.cancel()
		err = errors.Join(err, b.cleanupStartupFailure(true))
		return err
	}

	b.relay = b.startRelay(sig)
	b.started = true
	b.info("nethernet broadcaster started", "network_id", signalingNetworkID(sig), "target", b.conf.JoinURL)

	go b.sessionLoop()
	presenceClients := b.presenceClients()
	b.debug("starting presence updates", "count", len(presenceClients), "xuids", presenceClientXUIDs(presenceClients))
	for _, client := range presenceClients {
		go client.Run(b.ctx, b.log)
	}
	go b.uploadGalleryWithTimeout()
	if b.conf.FriendSync != nil && hasSocialClient(b.conf.XBLClient) {
		b.debug("starting friend sync",
			"auto_friend", b.conf.FriendSync.AutoFriend,
			"initial_invite", b.conf.FriendSync.InitialInvite,
			"expiry_enabled", b.conf.FriendSync.ExpiryEnabled,
		)
		syncer := b.friendSyncer()
		syncer.Trigger = b.startSocialSubscription(b.conf.XBLClient, b.conf.FriendSync, b.log)
		go syncer.Run(b.ctx)
	}
	b.startSubAccountFriendSync()
	go b.logSocialSummary()
	return nil
}

// enabledSubAccounts returns the first enabled configuration for each ID and
// reports duplicate enabled IDs. All runtime consumers use this selection so
// sessions, presence, friend sync, gallery, and recovery cannot disagree.
func (b *Broadcaster) enabledSubAccounts() (accounts []*SubAccountConfig, duplicates []string) {
	seen := make(map[string]struct{}, len(b.conf.SubAccounts))
	for i := range b.conf.SubAccounts {
		account := &b.conf.SubAccounts[i]
		if !account.Enabled {
			continue
		}
		if _, duplicate := seen[account.ID]; duplicate {
			duplicates = append(duplicates, account.ID)
			continue
		}
		seen[account.ID] = struct{}{}
		accounts = append(accounts, account)
	}
	return accounts, duplicates
}

// subAccountFriendSyncActive reports whether any enabled sub-account runs a
// friend syncer sharing the primary's history store.
func (b *Broadcaster) subAccountFriendSyncActive() bool {
	accounts, _ := b.enabledSubAccounts()
	for _, account := range accounts {
		if !subAccountHasXBLCredentials(*account) {
			continue
		}
		if account.FriendSync != nil || b.conf.FriendSync != nil {
			return true
		}
	}
	return false
}

// startSubAccountFriendSync runs a friend syncer per enabled sub-account so
// people who friend a sub-account are followed back. Sub-accounts without an
// explicit FriendSync configuration inherit the primary account's.
func (b *Broadcaster) startSubAccountFriendSync() {
	accounts, _ := b.enabledSubAccounts()
	for _, account := range accounts {
		if !hasSocialClient(account.XBLClient) {
			continue
		}
		conf := account.FriendSync
		if conf == nil {
			conf = b.conf.FriendSync
		}
		if conf == nil {
			continue
		}
		syncLog := b.log.With("sub_account", account.ID)
		syncer := FriendSyncer{
			Client:   b.friendClientFor(account.XBLClient),
			Config:   *conf,
			History:  b.conf.FriendHistory,
			Notifier: b.conf.Notifier,
			OwnXUIDs: b.ownXUIDs,
			Trigger:  b.startSocialSubscription(account.XBLClient, conf, syncLog),
			Log:      syncLog,
		}
		if conf.InitialInvite {
			syncer.Inviter = &subAccountInviter{b: b, id: account.ID}
		}
		b.debug("starting sub-account friend sync", "sub_account", account.ID)
		go syncer.Run(b.ctx)
	}
}

// logSocialSummary logs the authenticated account and its friend usage at
// startup, mirroring MCXboxBroadcast's "N/2000 friends" line. The count comes
// from the friend list like MCXboxBroadcast; the social summary's
// targetFollowingCount is unreliable for the caller's own profile.
func (b *Broadcaster) logSocialSummary() {
	if !hasSocialClient(b.conf.XBLClient) {
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	defer cancel()
	friends, err := b.friendClientFor(b.conf.XBLClient).Friends(ctx)
	if err != nil {
		b.debug("fetch friend list for summary", "err", err)
		return
	}
	b.info("authenticated to xbox live",
		"gamertag", b.hostNameFallback(),
		"xuid", b.primaryXUID(),
		"friends", fmt.Sprintf("%d/2000", len(friends)),
	)
}

// presenceClients builds the list of Xbox presence clients for heartbeat updates.
func (b *Broadcaster) presenceClients() []PresenceClient {
	var clients []PresenceClient
	if b.conf.XBLClient != nil && b.conf.XBLClient.Presence() != nil {
		clients = append(clients, PresenceClient{
			XUID:     b.primaryXUID(),
			Presence: b.conf.XBLClient.Presence(),
		})
	}
	accounts, _ := b.enabledSubAccounts()
	for _, account := range accounts {
		if account.XBLClient == nil {
			continue
		}
		xuid := accountXUID(*account)
		if xuid == "" {
			continue
		}
		presenceClient := account.XBLClient.Presence()
		if presenceClient == nil {
			continue
		}
		clients = append(clients, PresenceClient{
			XUID:     xuid,
			Presence: presenceClient,
		})
	}
	return clients
}

// ownXUIDs returns the primary and every enabled sub-account XUID, published or not.
func (b *Broadcaster) ownXUIDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	xuids := []string{b.primaryXUID()}
	accounts, _ := b.enabledSubAccounts()
	for _, account := range accounts {
		xuids = append(xuids, accountXUID(*account))
	}
	return xuids
}

// friendSyncer creates a FriendSyncer from the broadcaster's current config.
func (b *Broadcaster) friendSyncer() FriendSyncer {
	syncer := FriendSyncer{
		Client:   b.friendClientFor(b.conf.XBLClient),
		Config:   *b.conf.FriendSync,
		History:  b.conf.FriendHistory,
		Notifier: b.conf.Notifier,
		// Pruning compares the store against the primary's friend list only,
		// so it must stay off while sub-account syncers share the store:
		// people who only friended a sub-account would be pruned and re-seeded
		// with a fresh expiry clock every pass.
		PruneHistory: !b.subAccountFriendSyncActive(),
		OwnXUIDs:     b.ownXUIDs,
		Log:          b.log,
	}
	if b.conf.FriendSync.InitialInvite {
		syncer.Inviter = &broadcasterInviter{b: b}
	}
	return syncer
}

// startRelay relays offers received on sig until the next recreation or Close.
func (b *Broadcaster) startRelay(sig nethernet.Signaling) *httpRelay {
	recorder, _ := b.conf.FriendHistory.(HistoryRecorder)
	return startHTTPRelay(b.ctx, sig, &httpRelay{
		joinURL: b.conf.JoinURL,
		client: endpoint.ClientConfig{
			HTTPClient: b.conf.HTTPClient,
			Logger:     b.log,
			ClientInfo: endpoint.ClientInfo{Version: b.advertisedVersion(), Protocol: int(b.advertisedProtocol()), Platform: relayClientPlatform},
		},
		verify: func(ctx context.Context, sdp string) (offerIdentity, error) {
			verifier, err := b.tokens.get(ctx)
			if err != nil {
				return offerIdentity{}, err
			}
			return verifyOfferIdentity(ctx, sdp, verifier)
		},
		history: recorder,
		log:     b.log,
	})
}

// wrapAnnouncer applies logging and the shared signaling connection to a
// freshly created announcer.
func (b *Broadcaster) wrapAnnouncer(announcer room.Announcer, log *slog.Logger) room.Announcer {
	announcer = loggingAnnouncer{Announcer: announcer, log: log}
	if b.sessionConnection != nil {
		announcer = signalingConnectionAnnouncer{Announcer: announcer, connection: *b.sessionConnection}
	}
	return announcer
}

// primaryXBLClient returns or lazily creates the primary Xbox Live client.
func (b *Broadcaster) primaryXBLClient(ctx context.Context) (*xsapi.Client, error) {
	if b.conf.XBLClient != nil {
		return b.conf.XBLClient, nil
	}
	if b.xblClient != nil {
		return b.xblClient, nil
	}
	if b.conf.XBLTokenSource == nil {
		return nil, errors.New("xbox live token source is required")
	}
	client, err := NewXSAPIClient(ctx, b.conf.XBLTokenSource, b.conf.HTTPClient, b.log)
	if err != nil {
		return nil, fmt.Errorf("create xbox live client: %w", err)
	}
	b.xblClient = client
	b.conf.XBLClient = client
	b.createdXBLClients = append(b.createdXBLClients, client)
	return client, nil
}

// subAccountXBLClient returns or lazily creates an Xbox Live client for a sub-account.
func (b *Broadcaster) subAccountXBLClient(ctx context.Context, account *SubAccountConfig) (*xsapi.Client, error) {
	if account.XBLClient != nil {
		return account.XBLClient, nil
	}
	if account.XBLTokenSource == nil {
		return nil, errors.New("sub-account xbox live token source is required")
	}
	client, err := NewXSAPIClient(ctx, account.XBLTokenSource, b.conf.HTTPClient, b.log.With("sub_account", account.ID))
	if err != nil {
		return nil, fmt.Errorf("create sub-account xbox live client: %w", err)
	}
	account.XBLClient = client
	b.createdXBLClients = append(b.createdXBLClients, client)
	return client, nil
}

// minecraftTokenSource returns or lazily creates the Minecraft service token source.
func (b *Broadcaster) minecraftTokenSource(ctx context.Context) (service.TokenSource, error) {
	if b.conf.MinecraftTokenSource != nil {
		return b.conf.MinecraftTokenSource, nil
	}
	if b.minecraftTokens != nil {
		return b.minecraftTokens, nil
	}
	client, err := b.primaryXBLClient(ctx)
	if err != nil {
		return nil, err
	}
	tokens, err := newMinecraftTokenSource(ctx, client, b.conf.HTTPClient, b.log)
	if err != nil {
		return nil, err
	}
	b.minecraftTokens = tokens
	b.conf.MinecraftTokenSource = tokens
	return tokens, nil
}

// primaryXUID returns the primary account's XUID from config or client.
func (b *Broadcaster) primaryXUID() string {
	if b.conf.XUID != "" {
		return b.conf.XUID
	}
	if xuid := clientXUID(b.conf.XBLClient); xuid != "" {
		return xuid
	}
	return clientXUID(b.xblClient)
}

// accountXUID returns a sub-account's XUID from config or client.
func accountXUID(account SubAccountConfig) string {
	if account.XUID != "" {
		return account.XUID
	}
	return clientXUID(account.XBLClient)
}

// subAccountHasXBLCredentials reports whether a sub-account has Xbox credentials.
func subAccountHasXBLCredentials(account SubAccountConfig) bool {
	return account.XBLClient != nil || account.XBLTokenSource != nil
}

// clientXUID extracts the XUID from an xsapi.Client, returning empty if nil.
func clientXUID(client *xsapi.Client) string {
	if client == nil {
		return ""
	}
	return client.UserInfo().XUID
}

// hasSocialClient reports whether an account has initialized its social API client.
func hasSocialClient(client *xsapi.Client) bool {
	return client != nil && client.Social() != nil
}

// friendClientFor wraps an Xbox client into a FriendClient for social API calls.
func (b *Broadcaster) friendClientFor(client *xsapi.Client) FriendClient {
	return FriendClient{Social: client.Social()}
}

// broadcasterInviter resolves the current MPSD session dynamically for friend invites.
type broadcasterInviter struct {
	b *Broadcaster
}

// Invite snapshots the active MPSD session under the mutex and sends a game invite.
func (i *broadcasterInviter) Invite(ctx context.Context, xuid, titleID string) error {
	i.b.mu.Lock()
	if !i.b.started {
		i.b.mu.Unlock()
		return errors.New("invite: broadcaster not started")
	}
	announcer, ok := xblAnnouncer(i.b.announcer)
	if !ok {
		i.b.mu.Unlock()
		return errors.New("invite: no active MPSD session")
	}
	announcer.Lock()
	session := announcer.Session
	announcer.Unlock()
	i.b.mu.Unlock()
	if session == nil || session.Context().Err() != nil {
		return errors.New("invite: no active MPSD session")
	}
	_, err := session.Invite(ctx, xuid, titleID)
	return err
}

// subAccountInviter sends friend invites through a sub-account's independently
// published session, resolved dynamically so invites survive session rebuilds.
type subAccountInviter struct {
	b  *Broadcaster
	id string
}

func (i *subAccountInviter) Invite(ctx context.Context, xuid, titleID string) error {
	i.b.mu.Lock()
	announcer := i.b.subAnnouncersByID[i.id]
	i.b.mu.Unlock()
	xbl, ok := xblAnnouncer(announcer)
	if !ok {
		return errors.New("invite: sub-account has not published a session")
	}
	xbl.Lock()
	session := xbl.Session
	xbl.Unlock()
	if session == nil {
		return errors.New("invite: sub-account has not published a session")
	}
	_, err := session.Invite(ctx, xuid, titleID)
	return err
}

// Invite sends a Minecraft game invite to xuid through the broadcaster's
// active MPSD session.
func (b *Broadcaster) Invite(ctx context.Context, xuid string) error {
	return (&broadcasterInviter{b: b}).Invite(ctx, xuid, strconv.FormatInt(TitleID, 10))
}

// loggingAnnouncer wraps an announcer with debug-level status logging.
type loggingAnnouncer struct {
	room.Announcer
	log *slog.Logger
}

// Announce logs the status at debug level before delegating to the wrapped announcer.
func (a loggingAnnouncer) Announce(ctx context.Context, status room.Status) error {
	debugRoomStatus(a.log, "publishing xbox live session status", status)
	return a.Announcer.Announce(ctx, status)
}

// xblAnnouncer unwraps announcer wrappers to find the underlying XBLAnnouncer.
func xblAnnouncer(announcer room.Announcer) (*room.XBLAnnouncer, bool) {
	switch a := unwrapAnnouncer(announcer).(type) {
	case *room.XBLAnnouncer:
		return a, true
	case *sessionNonceAnnouncer:
		return a.XBLAnnouncer, true
	default:
		return nil, false
	}
}

// unwrapAnnouncer strips the broadcaster's logging and connection wrappers.
func unwrapAnnouncer(announcer room.Announcer) room.Announcer {
	for {
		switch a := announcer.(type) {
		case loggingAnnouncer:
			announcer = a.Announcer
		case signalingConnectionAnnouncer:
			announcer = a.Announcer
		default:
			return announcer
		}
	}
}

// signalingFor returns or creates the nethernet signaling connection.
func (b *Broadcaster) signalingFor(ctx context.Context) (nethernet.Signaling, error) {
	if b.conf.Signaling != nil {
		b.debug("using configured nethernet signaling", "signaling_type", fmt.Sprintf("%T", b.conf.Signaling))
		return b.conf.Signaling, nil
	}
	if b.conf.SignalingFactory != nil {
		b.debug("creating nethernet signaling from factory")
		sig, err := b.conf.SignalingFactory(ctx, b.conf)
		if err == nil {
			b.debug("created nethernet signaling from factory", "signaling_type", fmt.Sprintf("%T", sig), "network_id", signalingNetworkID(sig))
		}
		return sig, err
	}
	timeout := b.signalingDialTimeout()
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	b.debug("dialing nethernet signaling", "timeout", timeout)
	conf := b.defaultSignalingConfig()
	conf.httpClient = signalingStartupHTTPClient(conf.httpClient, timeout)
	resultCh := make(chan defaultSignalingResult)
	go func() {
		result := dialDefaultSignaling(dialCtx, conf)
		select {
		case resultCh <- result:
		case <-dialCtx.Done():
			closeDefaultSignalingResult(result)
		}
	}()
	select {
	case result := <-resultCh:
		if dialCtx.Err() != nil {
			closeDefaultSignalingResult(result)
			return nil, fmt.Errorf("dial nethernet signaling: %w", dialCtx.Err())
		}
		if result.err != nil {
			if result.createdClient != nil {
				_ = result.createdClient.Close()
			}
			return nil, result.err
		}
		b.cacheDefaultSignalingResult(result)
		return result.signaling, nil
	case <-dialCtx.Done():
		return nil, fmt.Errorf("dial nethernet signaling: %w", dialCtx.Err())
	}
}

// defaultSignalingConfig builds the signaling config from the broadcaster's current state.
func (b *Broadcaster) defaultSignalingConfig() defaultSignalingConfig {
	client := b.conf.XBLClient
	if client == nil {
		client = b.xblClient
	}
	tokens := b.conf.MinecraftTokenSource
	if tokens == nil {
		tokens = b.minecraftTokens
	}
	return defaultSignalingConfig{
		log:             b.log,
		httpClient:      b.conf.HTTPClient,
		xblClient:       client,
		xblTokenSource:  b.conf.XBLTokenSource,
		minecraftTokens: tokens,
	}
}

// dialDefaultSignaling dials a nethernet signaling connection using the given config.
func dialDefaultSignaling(ctx context.Context, conf defaultSignalingConfig) defaultSignalingResult {
	debugLog(conf.log, "creating minecraft token source for signaling")
	src, createdClient, err := conf.minecraftTokenSource(ctx)
	if err != nil {
		return defaultSignalingResult{createdClient: createdClient, err: err}
	}
	debugLog(conf.log, "created minecraft token source for signaling")
	debugLog(conf.log, "dialing jsonrpc signaling websocket")
	sig, err := signaling.Dialer{Log: conf.log, HTTPClient: conf.httpClient}.DialContext(ctx, src)
	if err != nil {
		return defaultSignalingResult{createdClient: createdClient, err: err}
	}
	return defaultSignalingResult{signaling: sig, minecraft: src, createdClient: createdClient}
}

// minecraftTokenSource returns or creates a Minecraft token source for signaling.
func (conf defaultSignalingConfig) minecraftTokenSource(ctx context.Context) (service.TokenSource, *xsapi.Client, error) {
	if conf.minecraftTokens != nil {
		return conf.minecraftTokens, nil, nil
	}
	client := conf.xblClient
	var createdClient *xsapi.Client
	if client == nil {
		if conf.xblTokenSource == nil {
			return nil, nil, errors.New("xbox live token source is required")
		}
		var err error
		client, err = NewXSAPIClient(ctx, conf.xblTokenSource, conf.httpClient, conf.log)
		if err != nil {
			return nil, nil, fmt.Errorf("create xbox live client: %w", err)
		}
		createdClient = client
	}
	tokens, err := newMinecraftTokenSource(ctx, client, conf.httpClient, conf.log)
	if err != nil {
		return nil, createdClient, err
	}
	return tokens, createdClient, nil
}

// cacheDefaultSignalingResult stores lazily-created clients and tokens from signaling dial.
func (b *Broadcaster) cacheDefaultSignalingResult(result defaultSignalingResult) {
	if result.createdClient != nil {
		b.xblClient = result.createdClient
		b.conf.XBLClient = result.createdClient
		b.createdXBLClients = append(b.createdXBLClients, result.createdClient)
	}
	if b.conf.MinecraftTokenSource == nil && b.minecraftTokens == nil && result.minecraft != nil {
		b.minecraftTokens = result.minecraft
		b.conf.MinecraftTokenSource = result.minecraft
	}
}

// closeDefaultSignalingResult closes resources from a signaling dial result.
func closeDefaultSignalingResult(result defaultSignalingResult) {
	if result.signaling != nil {
		if c, ok := result.signaling.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
	if result.createdClient != nil {
		_ = result.createdClient.Close()
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

// newAnnouncer creates the primary MPSD session announcer; an empty name picks a random one.
func (b *Broadcaster) newAnnouncer(ctx context.Context, name string) (room.Announcer, error) {
	if b.announcerFactory != nil {
		b.debug("using configured xbox live announcer factory")
		return b.announcerFactory(b), nil
	}
	client, err := b.primaryXBLClient(ctx)
	if err != nil {
		return nil, err
	}
	mpsdClient := client.MPSD()
	if mpsdClient == nil {
		return nil, errors.New("xbox live MPSD client is nil")
	}
	ref := mpsd.SessionReference{
		ServiceConfigID: serviceConfigUUID,
		TemplateName:    TemplateName,
		Name:            name,
	}
	if ref.Name == "" {
		ref.Name = strings.ToUpper(uuid.NewString())
	}
	b.sessionRef = ref
	pub := b.conf.PublishConfig
	b.debug("configured xbox live session",
		"service_config_id", ref.ServiceConfigID.String(),
		"template", ref.TemplateName,
		"session_name", ref.Name,
		"join_restriction", defaultString(pub.JoinRestriction, mpsd.SessionRestrictionFollowed),
		"read_restriction", defaultString(pub.ReadRestriction, mpsd.SessionRestrictionFollowed),
		"xuid", b.primaryXUID(),
	)
	return newSessionNonceAnnouncer(&room.XBLAnnouncer{
		Client:           mpsdClient,
		SessionReference: ref,
		PublishConfig:    pub,
	}, b.primaryXUID(), b.log, b.membersChanged), nil
}

// membersChanged requests an early health check once a session nears its member cap.
func (b *Broadcaster) membersChanged(count int) {
	if count < sessionRotateMembers {
		return
	}
	select {
	case b.healthCheck <- struct{}{}:
	default:
	}
}

// startSubAccounts publishes an independently owned session for every enabled
// sub-account. All sessions advertise the same NetherNet signaling connection,
// so each account is directly joinable while routing players to one listener.
// A failing sub-account is logged and skipped so it cannot take down the
// broadcaster, matching MCXboxBroadcast.
func (b *Broadcaster) startSubAccounts(ctx context.Context, status room.Status) error {
	accounts, duplicates := b.enabledSubAccounts()
	for _, id := range duplicates {
		b.log.Error("duplicate sub-account id; only the first configuration is used", "sub_account", id)
		b.notify(ctx, "Duplicate sub-account id "+id+": only the first configuration is used.")
	}
	for _, account := range accounts {
		b.debug("checking sub-account",
			"sub_account", account.ID,
			"enabled", account.Enabled,
			"has_xbox_credentials", subAccountHasXBLCredentials(*account),
			"xuid", accountXUID(*account),
		)
		if !subAccountHasXBLCredentials(*account) {
			b.log.Warn("sub-account skipped because xbox live credentials are missing", "sub_account", account.ID)
			continue
		}
		if err := b.startSubAccountBounded(ctx, account, status); err != nil {
			// A canceled context means the broadcaster is shutting down, not
			// that this or the remaining sub-accounts genuinely failed.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			b.log.Error("start sub-account; continuing without it", "sub_account", account.ID, "err", err)
			b.notify(ctx, "Sub-account "+account.ID+" failed to start: "+err.Error())
		}
	}
	return nil
}

// startSubAccountBounded runs startSubAccount under the per-account timeout.
// The context only scopes the publish requests; the sub-account's RTA
// connection and session outlive it.
func (b *Broadcaster) startSubAccountBounded(ctx context.Context, account *SubAccountConfig, status room.Status) error {
	timeout := b.subAccountStartTimeout
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return b.startSubAccount(ctx, account, status)
}

// startSubAccount prepares one sub-account and publishes its own session.
func (b *Broadcaster) startSubAccount(ctx context.Context, account *SubAccountConfig, status room.Status) error {
	if _, err := b.subAccountXBLClient(ctx, account); err != nil {
		return fmt.Errorf("prepare xbox live client: %w", err)
	}
	if account.XUID == "" {
		account.XUID = accountXUID(*account)
	}
	if account.XUID == "" {
		return errors.New("sub-account xuid unavailable")
	}
	if err := b.ensureSubAccountFriendship(ctx, *account); err != nil {
		return fmt.Errorf("prepare friendship: %w", err)
	}
	ref := mpsd.SessionReference{
		ServiceConfigID: serviceConfigUUID,
		TemplateName:    TemplateName,
		Name:            strings.ToUpper(uuid.NewString()),
	}
	b.debug("publishing independent sub-account session",
		"sub_account", account.ID,
		"xuid", account.XUID,
		"session_name", ref.Name,
	)
	announcer, err := b.newSubAccountAnnouncer(ctx, *account, ref)
	if err != nil {
		return fmt.Errorf("create announcer: %w", err)
	}
	announcer = b.wrapAnnouncer(announcer, b.log.With("sub_account", account.ID))
	if err := announcer.Announce(ctx, subAccountStatus(status, account.XUID)); err != nil {
		_ = announcer.Close()
		return fmt.Errorf("announce session: %w", err)
	}
	// b.mu is held by Start for the whole startup sequence, so the session
	// bookkeeping mutates directly; locking here would self-deadlock.
	if b.subAnnouncersByID == nil {
		b.subAnnouncersByID = make(map[string]room.Announcer)
	}
	b.subAnnouncers = append(b.subAnnouncers, publishedSubAccount{
		id:        account.ID,
		xuid:      account.XUID,
		announcer: announcer,
	})
	b.subAnnouncersByID[account.ID] = announcer
	b.debug("published independent sub-account session", "sub_account", account.ID, "xuid", account.XUID)
	return nil
}

// newSubAccountAnnouncer creates an MPSD announcer owned by account. Its
// session reference is deliberately distinct from the primary session.
func (b *Broadcaster) newSubAccountAnnouncer(ctx context.Context, account SubAccountConfig, ref mpsd.SessionReference) (room.Announcer, error) {
	if b.subAccountAnnouncerFactory != nil {
		return b.subAccountAnnouncerFactory(ctx, account, ref)
	}
	if account.XBLClient == nil {
		return nil, errors.New("sub-account xbox live client is nil")
	}
	client := account.XBLClient.MPSD()
	if client == nil {
		return nil, errors.New("sub-account MPSD client is nil")
	}
	return newSessionNonceAnnouncer(&room.XBLAnnouncer{
		Client:           client,
		SessionReference: ref,
		PublishConfig:    account.PublishConfig,
	}, account.XUID, b.log.With("sub_account", account.ID), b.membersChanged), nil
}

// ensureSubAccountFriendship makes the primary and sub-account friends: the
// sub-account sends a request and the primary accepts it, like the Java sub-sessions.
func (b *Broadcaster) ensureSubAccountFriendship(ctx context.Context, account SubAccountConfig) error {
	primaryXUID := b.primaryXUID()
	subXUID := accountXUID(account)
	if primaryXUID == "" || subXUID == "" || primaryXUID == subXUID {
		return nil
	}
	if !hasSocialClient(b.conf.XBLClient) || !hasSocialClient(account.XBLClient) {
		return nil
	}
	sub := b.friendClientFor(account.XBLClient)
	if friends, err := sub.Friends(ctx); err == nil {
		for _, p := range friends {
			if p.XUID == primaryXUID {
				return nil
			}
		}
	}
	if err := sub.AddFriend(ctx, primaryXUID); err != nil {
		return fmt.Errorf("sub-account add primary: %w", err)
	}
	if err := b.friendClientFor(b.conf.XBLClient).AddFriend(ctx, subXUID); err != nil {
		return fmt.Errorf("primary add sub-account: %w", err)
	}
	if b.subAccountSettleDelay > 0 {
		// Give the friendship a moment to settle before publishing, like Java.
		select {
		case <-time.After(b.subAccountSettleDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// uploadGallery uploads the configured showcase image for the primary account
// and each enabled sub-account. Accounts fail independently.
func (b *Broadcaster) uploadGallery(ctx context.Context) {
	b.galleryMu.Lock()
	defer b.galleryMu.Unlock()

	cfg := b.conf.Gallery
	if cfg == nil || !cfg.Enabled {
		return
	}
	if _, err := os.Stat(cfg.ImagePath); errors.Is(err, os.ErrNotExist) {
		return
	}
	src := cfg.TokenSource
	primaryCtx, cancel := b.galleryAccountContext(ctx)
	if src == nil {
		tokens, err := b.minecraftTokenSource(b.sharedTokenSourceContext(primaryCtx))
		if err != nil {
			b.log.Warn("minecraft services token source unavailable", "err", err)
			b.notify(primaryCtx, "Showcase image upload skipped: Minecraft services token source is unavailable.")
		} else {
			src = tokens
		}
	}
	if src != nil {
		if xuid := b.primaryXUID(); xuid == "" {
			b.log.Warn("gallery skipped because token XUID is empty")
			b.notify(primaryCtx, "Showcase image upload skipped: Xbox profile XUID is empty.")
		} else {
			b.uploadGalleryAccount(primaryCtx, cfg, "", xuid, src)
		}
	}
	cancel()

	accounts, _ := b.enabledSubAccounts()
	for _, account := range accounts {
		if !subAccountHasXBLCredentials(*account) {
			continue
		}
		xuid := accountXUID(*account)
		if xuid == "" {
			b.log.Warn("sub-account gallery skipped because xuid is empty", "sub_account", account.ID)
			continue
		}
		accountCtx, cancel := b.galleryAccountContext(ctx)
		src, err := b.subAccountMinecraftTokenSource(b.sharedTokenSourceContext(accountCtx), account)
		if err != nil {
			b.log.Warn("sub-account minecraft services token source unavailable", "sub_account", account.ID, "err", err)
			b.notify(accountCtx, "Showcase image upload skipped for sub-account "+account.ID+": Minecraft services token source is unavailable.")
			cancel()
			continue
		}
		b.uploadGalleryAccount(accountCtx, cfg, account.ID, xuid, src)
		cancel()
	}
}

func (b *Broadcaster) galleryAccountContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := b.galleryUploadTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func (b *Broadcaster) uploadGalleryAccount(ctx context.Context, cfg *GalleryConfig, accountID, xuid string, src service.TokenSource) {
	client := GalleryClient{TokenSource: src, Client: cfg.Client, Log: b.log}
	if client.Client == nil {
		client.Client = b.conf.HTTPClient
	}
	args := []any{"path", cfg.ImagePath, "delete_other", cfg.DeleteOtherImages, "xuid", xuid}
	if accountID != "" {
		args = append(args, "sub_account", accountID)
	}
	b.info("setting showcase image", args...)
	result, err := client.SetShowcaseResult(ctx, xuid, cfg.ImagePath, cfg.DeleteOtherImages)
	if err != nil {
		b.log.Error("set showcase image", append(args, "err", err)...)
		if accountID == "" {
			b.notify(ctx, "Showcase image upload failed: "+err.Error())
		} else {
			b.notify(ctx, "Showcase image upload failed for sub-account "+accountID+": "+err.Error())
		}
		return
	}
	if result.AlreadySet {
		b.info("showcase image is already set, skipping upload", append(args, "image_id", result.ImageID)...)
	}
	b.info("successfully set showcase image", append(args, "image_id", result.ImageID, "uploaded", result.Uploaded)...)
}

func (b *Broadcaster) subAccountMinecraftTokenSource(ctx context.Context, account *SubAccountConfig) (service.TokenSource, error) {
	if account.MinecraftTokenSource != nil {
		return account.MinecraftTokenSource, nil
	}
	client, err := b.subAccountXBLClient(ctx, account)
	if err != nil {
		return nil, err
	}
	if tokens := b.subMinecraftTokens[client]; tokens != nil {
		return tokens, nil
	}
	tokens, err := newMinecraftTokenSource(ctx, client, b.conf.HTTPClient, b.log.With("sub_account", account.ID))
	if err != nil {
		return nil, err
	}
	if b.subMinecraftTokens == nil {
		b.subMinecraftTokens = make(map[*xsapi.Client]service.TokenSource)
	}
	b.subMinecraftTokens[client] = tokens
	return tokens, nil
}

// sharedTokenSourceContext returns the broadcaster's context or the fallback if unavailable.
func (b *Broadcaster) sharedTokenSourceContext(fallback context.Context) context.Context {
	if b.ctx != nil && b.ctx.Err() == nil {
		return b.ctx
	}
	if fallback != nil {
		return fallback
	}
	return context.Background()
}

// debug logs a message at debug level if the logger is set.
func (b *Broadcaster) debug(msg string, args ...any) {
	debugLog(b.log, msg, args...)
}

// debugLog logs a message at debug level using the given logger if non-nil.
func debugLog(log *slog.Logger, msg string, args ...any) {
	if log != nil {
		log.Debug(msg, args...)
	}
}

// info logs a message at info level if the logger is set.
func (b *Broadcaster) info(msg string, args ...any) {
	if b.log != nil {
		b.log.Info(msg, args...)
	}
}

// warn logs a message at warn level if the logger is set.
func (b *Broadcaster) warn(msg string, args ...any) {
	if b.log != nil {
		b.log.Warn(msg, args...)
	}
}

// debugRoomStatus logs the room status at debug level.
func (b *Broadcaster) debugRoomStatus(msg string, status room.Status) {
	debugRoomStatus(b.log, msg, status)
}

// debugRoomStatus logs the room status at debug level using the given logger.
func debugRoomStatus(log *slog.Logger, msg string, status room.Status) {
	if log == nil || !log.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	log.Debug(msg, roomStatusLogArgs(status)...)
}

// roomStatusLogArgs builds slog key-value args from a room status.
func roomStatusLogArgs(status room.Status) []any {
	return []any{
		"host_name", status.HostName,
		"world_name", status.WorldName,
		"world_type", status.WorldType,
		"owner_id", status.OwnerID,
		"owner_id_set", status.OwnerID != "",
		"member_count", status.MemberCount,
		"max_member_count", status.MaxMemberCount,
		"broadcast_setting", status.BroadcastSetting,
		"joinability", status.Joinability,
		"protocol", status.Protocol,
		"version", status.Version,
		"title_id", status.TitleID,
		"transport_layer", status.TransportLayer,
		"lan_game", status.LanGame,
		"online_cross_platform_game", status.OnlineCrossPlatformGame,
		"cross_play_disabled", status.CrossPlayDisabled,
		"level_id_set", status.LevelID != "",
		"level_id_len", len(status.LevelID),
		"supported_connection_count", len(status.SupportedConnections),
		"supported_connections", roomConnectionLogValues(status.SupportedConnections),
	}
}

// roomConnectionLogValue is a loggable representation of a room connection.
type roomConnectionLogValue struct {
	ConnectionType int
	HostIPAddress  string
	HostPort       uint16
	NetherNetID    string
	NetherNetIDSet bool
	RakNetGUIDSet  bool
	PmsgID         string
	PmsgIDSet      bool
}

// roomConnectionLogValues converts room connections to loggable values.
func roomConnectionLogValues(connections []p2p.Connection) []roomConnectionLogValue {
	values := make([]roomConnectionLogValue, 0, len(connections))
	for _, connection := range connections {
		netherNetID := string(connection.NetherNetID)
		values = append(values, roomConnectionLogValue{
			ConnectionType: connection.Type,
			HostIPAddress:  connection.HostIPAddress,
			HostPort:       connection.HostPort,
			NetherNetID:    netherNetID,
			NetherNetIDSet: netherNetID != "" && netherNetID != "0",
			RakNetGUIDSet:  connection.RakNetGUID != "",
			PmsgID:         connection.PlayerMessagingID.String(),
			PmsgIDSet:      connection.PlayerMessagingID != uuid.Nil,
		})
	}
	return values
}

// signalingNetworkID returns the network ID from the signaling or an empty string if nil.
func signalingNetworkID(sig nethernet.Signaling) string {
	if sig == nil {
		return ""
	}
	return sig.NetworkID()
}

// presenceClientXUIDs extracts the XUIDs from a slice of presence clients.
func presenceClientXUIDs(clients []PresenceClient) []string {
	xuids := make([]string, 0, len(clients))
	for _, client := range clients {
		xuids = append(xuids, client.XUID)
	}
	return xuids
}

// uploadGalleryWithTimeout runs gallery uploads with an independent timeout
// for each account.
func (b *Broadcaster) uploadGalleryWithTimeout() {
	b.uploadGallery(b.ctx)
}

// notify sends a notification via the configured notifier, if any.
func (b *Broadcaster) notify(ctx context.Context, message string) {
	if b.conf.Notifier == nil {
		return
	}
	notifyCtx := ctx
	if notifyCtx == nil || notifyCtx.Err() != nil {
		notifyCtx = b.ctx
	}
	if notifyCtx == nil || notifyCtx.Err() != nil {
		notifyCtx = context.Background()
	}
	notifyCtx, cancel := context.WithTimeout(notifyCtx, 15*time.Second)
	defer cancel()
	if err := b.conf.Notifier.Notify(notifyCtx, message); err != nil {
		b.log.Error("send notification", "err", err)
	}
}

// notifySessionUpdateFailure notifies about a session update failure.
func (b *Broadcaster) notifySessionUpdateFailure(ctx context.Context, err error) {
	b.notify(ctx, "Xbox session update failed: "+err.Error())
}

const (
	// sessionRotateMembers rotates before the 30-member template cap so joins
	// in progress still fit; players stay members while they play.
	sessionRotateMembers = 28
	// sessionRotateGrace keeps a rotated-out session joinable for players mid-join.
	sessionRotateGrace = 30 * time.Second
	// sessionUpdateFailureLimit is how many consecutive update failures
	// trigger a full session recreation.
	sessionUpdateFailureLimit = 3
)

// subAccountUpdateError reports that the primary session updated successfully
// but one or more sub-account sessions could not be recovered.
type subAccountUpdateError struct {
	err error
}

// Error returns the underlying sub-account recovery failures.
func (e *subAccountUpdateError) Error() string {
	return e.err.Error()
}

// Unwrap exposes the underlying sub-account recovery failures.
func (e *subAccountUpdateError) Unwrap() error {
	return e.err
}

// countsAsPrimaryUpdateFailure reports whether err should contribute to the
// failure count that recreates the primary session and shared signaling stack.
func countsAsPrimaryUpdateFailure(err error) bool {
	if err == nil {
		return false
	}
	var subErr *subAccountUpdateError
	return !errors.As(err, &subErr)
}

type sessionHealthIssue struct {
	reason       string
	subAccountID string
	activity     *publishedActivity
	rotate       bool // the session is nearly full rather than broken
}

// recoverSessionHealthIssue replaces the unhealthy sub-account session without
// touching the primary session or shared signaling connection.
func (b *Broadcaster) recoverSessionHealthIssue(ctx context.Context, issue sessionHealthIssue) error {
	if issue.subAccountID == "" {
		return errors.New("sub-account id is empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if issue.activity != nil {
		current := false
		for _, sub := range b.subAnnouncers {
			if sub.id != issue.subAccountID {
				continue
			}
			xbl, ok := xblAnnouncer(sub.announcer)
			if ok && xbl == issue.activity.announcer {
				xbl.Lock()
				current = xbl.Session == issue.activity.session && xbl.SessionReference == issue.activity.ref
				xbl.Unlock()
			}
		}
		if !current {
			return nil
		}
	}
	status, err := b.status(ctx)
	if err != nil {
		return fmt.Errorf("resolve session status: %w", err)
	}
	return b.replaceSubAccountSession(ctx, issue.subAccountID, status, issue.rotate)
}

// sessionHealthIssue reports which published session needs recovery, or an
// empty issue when every session is healthy.
func (b *Broadcaster) sessionHealthIssue() sessionHealthIssue {
	b.mu.Lock()
	defer b.mu.Unlock()
	announcer, ok := xblAnnouncer(b.announcer)
	if !ok {
		return sessionHealthIssue{}
	}
	announcer.Lock()
	session := announcer.Session
	announcer.Unlock()
	if session == nil {
		return sessionHealthIssue{}
	}
	if session.Context().Err() != nil {
		return sessionHealthIssue{reason: "mpsd session lost"}
	}
	if count := sessionMemberCount(session); count >= sessionRotateMembers {
		return sessionHealthIssue{reason: fmt.Sprintf("session has %d/30 members", count), rotate: true}
	}
	for _, sub := range b.subAnnouncers {
		xbl, ok := xblAnnouncer(sub.announcer)
		if !ok {
			continue
		}
		xbl.Lock()
		session := xbl.Session
		xbl.Unlock()
		if session != nil && session.Context().Err() != nil {
			return sessionHealthIssue{reason: "sub-account session lost", subAccountID: sub.id}
		}
		if session != nil {
			if count := sessionMemberCount(session); count >= sessionRotateMembers {
				return sessionHealthIssue{
					reason:       fmt.Sprintf("sub-account session has %d/30 members", count),
					subAccountID: sub.id,
					rotate:       true,
				}
			}
		}
	}
	return sessionHealthIssue{}
}

func sessionMemberCount(session *mpsd.Session) int {
	count := 0
	for range session.Members() {
		count++
	}
	return count
}

// rotateSession moves the primary activity to a fresh session on the same
// signaling connection and retires the old one, like the Java rotation.
func (b *Broadcaster) rotateSession(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started || b.recovering {
		return errors.New("broadcaster is not running")
	}
	status, err := b.status(ctx)
	if err != nil {
		return fmt.Errorf("resolve session status: %w", err)
	}
	announcer, err := b.newAnnouncer(ctx, "")
	if err != nil {
		return fmt.Errorf("create announcer: %w", err)
	}
	announcer = b.wrapAnnouncer(announcer, b.log)
	if err := announcer.Announce(ctx, status); err != nil {
		_ = announcer.Close()
		return fmt.Errorf("announce session: %w", err)
	}
	b.retire(b.announcer, b.log)
	b.announcer = announcer
	return nil
}

// sessionHider is implemented by announcers whose session can be made invite-only.
type sessionHider interface {
	hide(ctx context.Context) error
}

// retiredSession is a rotated-out session and when it was retired.
type retiredSession struct {
	announcer room.Announcer
	at        time.Time
}

// memberCounter reports a published session's member count; ok is false
// before the session is published.
type memberCounter interface {
	memberCount() (count int, ok bool)
}

// retire keeps a rotated-out session for players mid-join, then hides it if
// configured. Its members stay so they keep playing. The caller must hold b.mu.
func (b *Broadcaster) retire(announcer room.Announcer, log *slog.Logger) {
	b.retiredSessions = append(b.retiredSessions, retiredSession{announcer: announcer, at: time.Now()})
	if !b.conf.HideOldSessions {
		return
	}
	hider, ok := unwrapAnnouncer(announcer).(sessionHider)
	if !ok {
		return
	}
	grace := b.rotationGrace
	if grace <= 0 {
		grace = sessionRotateGrace
	}
	ctx := b.ctx
	go func() {
		select {
		case <-time.After(grace):
		case <-ctx.Done():
			return
		}
		hideCtx, cancel := xboxOperationContext(ctx)
		defer cancel()
		if err := hider.hide(hideCtx); err != nil {
			log.Warn("hide old session", "err", err)
			return
		}
		log.Debug("hid old session")
	}()
}

// closeDrainedSessions closes retired sessions past their grace period that
// only the bot still belongs to, so they stop holding RTA subscriptions.
func (b *Broadcaster) closeDrainedSessions() {
	b.mu.Lock()
	defer b.mu.Unlock()
	grace := b.rotationGrace
	if grace <= 0 {
		grace = sessionRotateGrace
	}
	kept := b.retiredSessions[:0]
	for _, retired := range b.retiredSessions {
		counter, ok := unwrapAnnouncer(retired.announcer).(memberCounter)
		count, published := 0, false
		if ok {
			count, published = counter.memberCount()
		}
		if !published || count > 1 || time.Since(retired.at) < grace {
			kept = append(kept, retired)
			continue
		}
		if err := retired.announcer.Close(); err != nil {
			b.warn("close drained session", "err", err)
			kept = append(kept, retired)
			continue
		}
		b.debug("closed drained session")
	}
	clear(b.retiredSessions[len(kept):])
	b.retiredSessions = kept
}

// recreateSession tears down and rebuilds signaling, sessions and the relay after a drop.
func (b *Broadcaster) recreateSession() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.ctx.Err() != nil || !b.started {
		return errors.New("broadcaster is shut down")
	}
	if b.relay != nil {
		b.relay.close()
		b.relay = nil
	}
	_ = b.cleanupPublishedSessions(true)
	if b.signaling != nil {
		if c, ok := b.signaling.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
	b.signaling = nil

	sig, err := b.signalingFor(b.ctx)
	if err != nil {
		return fmt.Errorf("re-create signaling: %w", err)
	}
	if sig.Context().Err() != nil {
		if c, ok := sig.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		return errors.New("re-create signaling: factory returned signaling with dead context")
	}
	b.signaling = sig
	b.debug("nethernet signaling re-created", "network_id", signalingNetworkID(sig))

	closeSignaling := func() {
		if c, ok := sig.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		b.signaling = nil
	}

	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	status, err := b.status(ctx)
	if err != nil {
		closeSignaling()
		return fmt.Errorf("re-create session status: %w", err)
	}
	announcer, err := b.newAnnouncer(ctx, b.conf.SessionName)
	if err != nil {
		closeSignaling()
		return fmt.Errorf("re-create announcer: %w", err)
	}
	connection, err := b.signalingConnection(sig)
	if err != nil {
		closeSignaling()
		return fmt.Errorf("re-create signaling connection: %w", err)
	}
	b.sessionConnection = connection
	b.announcer = b.wrapAnnouncer(announcer, b.log)
	if err := b.announcer.Announce(ctx, status); err != nil {
		closeSignaling()
		return fmt.Errorf("re-announce session: %w", err)
	}
	if err := b.startSubAccounts(b.ctx, status); err != nil {
		_ = b.cleanupPublishedSessions(true)
		closeSignaling()
		return fmt.Errorf("re-create sub-accounts: %w", err)
	}

	b.relay = b.startRelay(sig)
	b.debug("join relay restarted", "network_id", signalingNetworkID(sig), "target", b.conf.JoinURL)

	// Re-showcase the gallery image so a swapped file does not require a
	// process restart; the upload no-ops when the image is already set.
	go b.uploadGalleryWithTimeout()
	return nil
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
	status, err := b.status(ctx)
	if err != nil {
		return err
	}
	b.debugRoomStatus("resolved room status update", status)
	if err := b.announcer.Announce(ctx, status); err != nil {
		return err
	}
	type failedSubAccountUpdate struct {
		id  string
		err error
	}
	var failures []failedSubAccountUpdate
	for _, sub := range b.subAnnouncers {
		if err := sub.announcer.Announce(ctx, subAccountStatus(status, sub.xuid)); err != nil {
			failures = append(failures, failedSubAccountUpdate{id: sub.id, err: err})
		}
	}
	var recoveryErr error
	for _, failure := range failures {
		if err := b.replaceSubAccountSession(ctx, failure.id, status, false); err != nil {
			recoveryErr = errors.Join(recoveryErr, fmt.Errorf("replace sub-account %s after update failed (%v): %w", failure.id, failure.err, err))
			continue
		}
		b.warn("replaced sub-account session after update failure", "sub_account", failure.id, "cause", failure.err)
	}
	b.cleanupStaleSubAccountSessions()
	if recoveryErr != nil {
		return &subAccountUpdateError{err: recoveryErr}
	}
	// Matching MCXboxBroadcast, suppressSessionUpdateMessage only demotes the
	// periodic success log to debug level.
	if b.conf.SuppressSessionUpdateMessage {
		b.debug("updated session")
	} else {
		b.info("updated session")
	}
	return nil
}

// replaceSubAccountSession publishes a fresh session for id, then retires the
// old one when rotating or closes it otherwise. The caller must hold b.mu.
func (b *Broadcaster) replaceSubAccountSession(ctx context.Context, id string, status room.Status, rotate bool) error {
	oldIndex := -1
	var old room.Announcer
	for i, sub := range b.subAnnouncers {
		if sub.id == id {
			oldIndex = i
			old = sub.announcer
			break
		}
	}
	if oldIndex == -1 {
		return errors.New("stale sub-account session not found")
	}
	var account *SubAccountConfig
	accounts, _ := b.enabledSubAccounts()
	for _, candidate := range accounts {
		if candidate.ID == id {
			account = candidate
			break
		}
	}
	if account == nil {
		return errors.New("enabled sub-account configuration not found")
	}
	if err := b.startSubAccountBounded(ctx, account, status); err != nil {
		return err
	}
	b.subAnnouncers = append(b.subAnnouncers[:oldIndex], b.subAnnouncers[oldIndex+1:]...)
	if rotate {
		b.retire(old, b.log.With("sub_account", id))
		return nil
	}
	if err := old.Close(); err != nil {
		b.warn("close stale sub-account session", "sub_account", id, "err", err)
		b.staleSubAnnouncers = append(b.staleSubAnnouncers, old)
	}
	return nil
}

// cleanupStaleSubAccountSessions retries closing replaced sessions whose first
// close attempt failed. The caller must hold b.mu.
func (b *Broadcaster) cleanupStaleSubAccountSessions() {
	if len(b.staleSubAnnouncers) == 0 {
		return
	}
	remaining := b.staleSubAnnouncers[:0]
	for _, announcer := range b.staleSubAnnouncers {
		if err := announcer.Close(); err != nil {
			b.warn("retry close stale sub-account session", "err", err)
			remaining = append(remaining, announcer)
		}
	}
	b.staleSubAnnouncers = remaining
}

// cleanupPublishedSessions closes independent sub-account sessions and
// optionally the primary announcer.
func (b *Broadcaster) cleanupPublishedSessions(closeAnnouncer bool) error {
	var err error
	var failed []room.Announcer
	for _, sub := range b.subAnnouncers {
		if closeErr := sub.announcer.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
			failed = append(failed, sub.announcer)
		}
	}
	stale := b.staleSubAnnouncers
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
	b.subAnnouncers = nil
	b.subAnnouncersByID = nil
	b.staleSubAnnouncers = failed
	if closeAnnouncer && b.announcer != nil {
		err = errors.Join(err, b.announcer.Close())
	}
	b.sessionConnection = nil
	return err
}

// cleanupStartupFailure cleans up all resources after a failed Start.
func (b *Broadcaster) cleanupStartupFailure(closeAnnouncer bool) error {
	err := b.cleanupPublishedSessions(closeAnnouncer)
	if b.signaling != nil {
		if c, ok := b.signaling.(interface{ Close() error }); ok {
			err = errors.Join(err, c.Close())
		}
	}
	err = errors.Join(err, b.closeCreatedXBLClients())
	return err
}

// closeCreatedXBLClients closes Xbox Live clients that were created during startup.
func (b *Broadcaster) closeCreatedXBLClients() error {
	if len(b.createdXBLClients) == 0 {
		return nil
	}
	created := createdXBLClientSet(b.createdXBLClients)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var err error
	for client := range created {
		err = errors.Join(err, client.CloseContext(ctx))
	}
	b.clearCreatedXBLClientReferences(created)
	b.createdXBLClients = nil
	return err
}

// createdXBLClientSet builds a set of created clients for reference clearing.
func createdXBLClientSet(clients []*xsapi.Client) map[*xsapi.Client]struct{} {
	created := make(map[*xsapi.Client]struct{}, len(clients))
	for _, client := range clients {
		if client != nil {
			created[client] = struct{}{}
		}
	}
	return created
}

// clearCreatedXBLClientReferences nils out config references to closed clients.
func (b *Broadcaster) clearCreatedXBLClientReferences(created map[*xsapi.Client]struct{}) {
	b.galleryMu.Lock()
	defer b.galleryMu.Unlock()

	primaryCreated := xblClientCreated(b.conf.XBLClient, created) || xblClientCreated(b.xblClient, created)
	if primaryCreated {
		b.conf.XBLClient = nil
		b.xblClient = nil
		if b.minecraftTokens != nil {
			b.minecraftTokens = nil
			b.conf.MinecraftTokenSource = nil
		}
	}
	for i := range b.conf.SubAccounts {
		if xblClientCreated(b.conf.SubAccounts[i].XBLClient, created) {
			b.conf.SubAccounts[i].XBLClient = nil
		}
	}
	b.subMinecraftTokens = nil
}

// xblClientCreated reports whether a client was created by the broadcaster.
func xblClientCreated(client *xsapi.Client, created map[*xsapi.Client]struct{}) bool {
	if client == nil {
		return false
	}
	_, ok := created[client]
	return ok
}

// Close stops the relay and removes the Xbox sessions.
func (b *Broadcaster) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started {
		return nil
	}
	b.cancel()
	// Release only our social handlers before closing any owned clients.
	b.socialWg.Wait()
	if b.relay != nil {
		b.relay.close()
		b.relay = nil
	}
	err := b.cleanupPublishedSessions(true)
	if b.signaling != nil {
		if c, ok := b.signaling.(interface{ Close() error }); ok {
			err = errors.Join(err, c.Close())
		}
	}
	err = errors.Join(err, b.closeCreatedXBLClients())
	b.started = false
	return err
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
