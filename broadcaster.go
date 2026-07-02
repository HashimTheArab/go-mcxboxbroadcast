package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/go-gl/mathgl/mgl32"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service"
	websocketsignaling "github.com/sandertv/gophertunnel/minecraft/service/signaling"
	"github.com/sandertv/gophertunnel/minecraft/service/signaling/messaging"
)

const (
	// go-nethernet starts this context before the first remote ICE candidate is
	// received, then reuses it for ICE, DTLS, SCTP, and channel readiness.
	defaultNetherNetConnTimeout = 30 * time.Second
	defaultTransferCloseTimeout = 15 * time.Second
	defaultSignalingDialTimeout = 15 * time.Second
)

// Broadcaster owns the Xbox Live session, NetherNet listener, and redirect
// loop for a published Bedrock server.
type Broadcaster struct {
	conf Config
	log  *slog.Logger

	announcer           room.Announcer
	listener            *minecraft.Listener
	signaling           nethernet.Signaling
	sessionRef          mpsd.SessionReference
	subSessions         []*mpsd.Session
	subSessionsByID     map[string]*mpsd.Session
	announcerFactory    func(*Broadcaster) room.Announcer
	subAccountPublisher func(context.Context, SubAccountConfig, mpsd.SessionReference, mpsd.PublishConfig) (*mpsd.Session, error)
	xblClient           *xsapi.Client
	minecraftTokens     service.TokenSource
	createdXBLClients   []*xsapi.Client

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu       sync.Mutex
	started  bool
	acceptWg sync.WaitGroup

	// lastQuery is the most recent successful target-server query, kept so
	// query failures fall back to real data instead of failing the update.
	lastQuery *minecraft.ServerStatus

	transferCloseTimeout time.Duration
	// subAccountStartTimeout bounds each sub-account's session join so a hung
	// directory or RTA call degrades to a skipped sub-account instead of
	// blocking broadcaster startup. Zero uses the default.
	subAccountStartTimeout time.Duration
	// subAccountSettleDelay is the wait after establishing a new sub-account
	// friendship before joining the session.
	subAccountSettleDelay time.Duration
}

type transferConn interface {
	WritePacket(packet.Packet) error
	ReadPacket() (packet.Packet, error)
	Flush() error
	Close() error
	SetReadDeadline(time.Time) error
	IdentityData() login.IdentityData
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
	mode            SignalingMode
	log             *slog.Logger
	httpClient      *http.Client
	xblClient       *xsapi.Client
	xblTokenSource  xsapi.TokenSource
	minecraftTokens service.TokenSource
}

// New validates conf and returns a Broadcaster.
func New(conf Config) (*Broadcaster, error) {
	if err := conf.Server.validate(); err != nil {
		return nil, err
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
		transferCloseTimeout:  conf.TransferCloseTimeout,
		subAccountSettleDelay: 5 * time.Second,
	}, nil
}

// Start publishes the Xbox session and starts accepting NetherNet clients.
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
	b.done = make(chan struct{})

	mode, err := b.signalingMode()
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(false))
	}
	b.debug("starting broadcaster",
		"signaling_mode", mode,
		"status_provider", b.conf.StatusProvider != nil,
		"sub_accounts", len(b.conf.SubAccounts),
		"friend_sync", b.conf.FriendSync != nil,
		"gallery", b.conf.Gallery != nil && b.conf.Gallery.Enabled,
	)
	b.warnWebSocketSignalingMode(mode)
	sig, err := b.signalingFor(b.ctx)
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(false))
	}
	b.signaling = sig
	b.debug("nethernet signaling ready", "signaling_mode", mode, "signaling_type", fmt.Sprintf("%T", sig), "network_id", signalingNetworkID(sig))

	status, err := b.status(b.ctx)
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(false))
	}
	b.debugRoomStatus("resolved room status", status)
	b.announcer, err = b.newAnnouncer(b.ctx)
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(false))
	}
	b.announcer = loggingAnnouncer{Announcer: b.announcer, log: b.log}
	connection, err := b.signalingConnection(b.ctx, sig)
	if err != nil {
		b.cancel()
		return errors.Join(err, b.cleanupStartupFailure(true))
	}
	if connection != nil {
		b.announcer = signalingConnectionAnnouncer{Announcer: b.announcer, connection: *connection}
		b.debug("using jsonrpc signaling", "nethernet_id", connection.NetherNetID, "pmsg_id", connection.PmsgID)
	} else if len(status.SupportedConnections) == 0 {
		// Without a signaling connection or caller-provided connections the
		// session would publish SupportedConnections: null and be unjoinable.
		b.cancel()
		err := errors.New("session would publish no supported connections and be unjoinable; use jsonrpc signaling or provide SupportedConnections via a status provider")
		return errors.Join(err, b.cleanupStartupFailure(true))
	}
	b.info("creating xbox live session")
	if err := b.announcer.Announce(b.ctx, status); err != nil {
		b.cancel()
		err = errors.Join(fmt.Errorf("announce session: %w", err), b.cleanupStartupFailure(true))
		return err
	}
	b.info("created xbox live session")
	b.debug("starting sub-account sessions", "count", len(b.conf.SubAccounts))
	if err := b.startSubAccounts(b.ctx); err != nil {
		b.cancel()
		err = errors.Join(err, b.cleanupStartupFailure(true))
		return err
	}

	b.registerNetherNetNetwork(sig, status)

	listenConf := b.minecraftListenConfig(status)
	b.debug("starting nethernet listener",
		"listen_network", "nethernet",
		"auth_disabled", listenConf.AuthenticationDisabled,
		"server_status_override", !b.roomListenConfig(status).DisableServerStatusOverride,
		"default_transport_timeout", b.usesDefaultNetherNetConnContext(),
		"transport_timeout", b.netherNetTransportTimeoutLogValue(),
	)
	l, err := listenConf.Listen("nethernet", "")
	if err != nil {
		b.cancel()
		err = errors.Join(fmt.Errorf("listen nethernet: %w", err), b.cleanupStartupFailure(true))
		return err
	}
	b.listener = l
	b.started = true
	b.info("nethernet broadcaster started", "network_id", signalingNetworkID(sig), "signaling_mode", mode)
	b.debug("started nethernet listener")

	startListener := b.listener
	b.acceptWg.Add(1)
	go func() {
		defer b.acceptWg.Done()
		b.acceptListener(startListener)
	}()
	go func() {
		<-b.ctx.Done()
		b.acceptWg.Wait()
		close(b.done)
	}()
	go b.updateLoop()
	go b.watchSignaling()
	presenceClients := b.presenceClients()
	b.debug("starting presence updates", "count", len(presenceClients), "xuids", presenceClientXUIDs(presenceClients))
	for _, client := range presenceClients {
		go client.Run(b.ctx, b.log)
	}
	go b.uploadGalleryWithTimeout()
	if b.conf.FriendSync != nil {
		b.debug("starting friend sync",
			"auto_follow", b.conf.FriendSync.AutoFollow,
			"auto_unfollow", b.conf.FriendSync.AutoUnfollow,
			"initial_invite", b.conf.FriendSync.InitialInvite,
			"expiry_enabled", b.conf.FriendSync.ExpiryEnabled,
		)
		go b.friendSyncer().Run(b.ctx)
	}
	b.startSubAccountFriendSync()
	go b.logSocialSummary()
	return nil
}

// subAccountFriendSyncActive reports whether any enabled sub-account runs a
// friend syncer sharing the primary's history store.
func (b *Broadcaster) subAccountFriendSyncActive() bool {
	for _, account := range b.conf.SubAccounts {
		if !account.Enabled || !subAccountHasXBLCredentials(account) {
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
	for _, account := range b.conf.SubAccounts {
		if !account.Enabled || account.XBLClient == nil {
			continue
		}
		conf := account.FriendSync
		if conf == nil {
			conf = b.conf.FriendSync
		}
		if conf == nil {
			continue
		}
		syncer := FriendSyncer{
			Client:   b.friendClientFor(account.XBLClient),
			Config:   *conf,
			History:  b.conf.FriendHistory,
			Notifier: b.conf.Notifier,
			Log:      b.log.With("sub_account", account.ID),
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
	clients := []PresenceClient{{
		XUID:   b.primaryXUID(),
		Client: authenticatedHTTPClient(b.conf.XBLClient, b.conf.HTTPClient),
	}}
	for _, account := range b.conf.SubAccounts {
		if !account.Enabled {
			continue
		}
		if !subAccountHasXBLCredentials(account) {
			continue
		}
		xuid := accountXUID(account)
		if xuid == "" {
			continue
		}
		clients = append(clients, PresenceClient{
			XUID:   xuid,
			Client: authenticatedHTTPClient(account.XBLClient, b.conf.HTTPClient),
		})
	}
	return clients
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
		Log:          b.log,
	}
	if b.conf.FriendSync.InitialInvite {
		syncer.Inviter = &broadcasterInviter{b: b}
	}
	return syncer
}

// roomStatusProvider returns the room status provider, falling back to config defaults.
func (b *Broadcaster) roomStatusProvider(status room.Status) room.StatusProvider {
	if b.conf.StatusProvider != nil {
		return normalizedStatusProvider{Provider: b.conf.StatusProvider, OwnerID: b.primaryXUID()}
	}
	return room.NewStatusProvider(status)
}

// roomListenConfig builds the room.ListenConfig for the nethernet listener.
func (b *Broadcaster) roomListenConfig(status room.Status) room.ListenConfig {
	return room.ListenConfig{
		Announcer:                   b.announcer,
		StatusProvider:              b.roomStatusProvider(status),
		DisableServerStatusOverride: true,
		Log:                         b.log,
	}
}

// minecraftListenConfig applies broadcaster defaults to a Minecraft listener.
// Client authentication follows ListenConfig.AuthenticationDisabled: chains
// are validated by default like MCXboxBroadcast, so player history only
// records verified XUIDs.
func (b *Broadcaster) minecraftListenConfig(status room.Status) minecraft.ListenConfig {
	conf := b.conf.ListenConfig
	conf.ErrorLog = b.log
	conf.StatusProvider = b.minecraftStatusProvider(status)
	conf.CompressionThreshold = -1
	conf.ForceDisableVibrantVisuals = true
	conf.ResourcePackWorldTemplateUUID = uuid.New()
	conf.ResourcePackWorldTemplateVersion = "*"
	if b.debugEnabled() && conf.PacketFunc == nil {
		conf.PacketFunc = b.logMinecraftPacket
	}
	return conf
}

// netherNetListenConfig returns the nethernet listen config with a default conn context applied.
func (b *Broadcaster) netherNetListenConfig() nethernet.ListenConfig {
	conf := b.conf.NetherNetListenConfig
	conf.AllowAnonymous = true
	if conf.ConnContext == nil {
		conf.ConnContext = defaultNetherNetConnContext
	}
	return conf
}

// registerNetherNetNetwork registers the "nethernet" network for the minecraft listener.
func (b *Broadcaster) registerNetherNetNetwork(sig nethernet.Signaling, status room.Status) {
	minecraft.RegisterNetwork("nethernet", func(l *slog.Logger) minecraft.Network {
		return room.Network{
			Network:      minecraft.NetherNet{Signaling: sig, ListenConfig: b.netherNetListenConfig(), Log: b.log},
			ListenConfig: b.roomListenConfig(status),
		}
	})
}

// usesDefaultNetherNetConnContext reports whether the default conn timeout is in use.
func (b *Broadcaster) usesDefaultNetherNetConnContext() bool {
	return b.conf.NetherNetListenConfig.ConnContext == nil
}

// netherNetTransportTimeoutLogValue returns a human-readable transport timeout for logging.
func (b *Broadcaster) netherNetTransportTimeoutLogValue() string {
	if b.usesDefaultNetherNetConnContext() {
		return defaultNetherNetConnTimeout.String()
	}
	return "custom"
}

// defaultNetherNetConnContext provides a 30s timeout context for nethernet connections.
func defaultNetherNetConnContext(parent context.Context, _ *nethernet.Conn) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, defaultNetherNetConnTimeout)
}

// minecraftStatusProvider returns the minecraft status provider for the listener.
func (b *Broadcaster) minecraftStatusProvider(status room.Status) minecraft.ServerStatusProvider {
	if b.conf.StatusProvider != nil {
		return roomMinecraftStatusProvider{Provider: b.conf.StatusProvider}
	}
	return minecraft.NewStatusProvider(status.WorldName, status.HostName)
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

// friendClientFor wraps an Xbox client into a FriendClient for social API calls.
func (b *Broadcaster) friendClientFor(client *xsapi.Client) FriendClient {
	return FriendClient{Client: authenticatedHTTPClient(client, b.conf.HTTPClient)}
}

// authenticatedHTTPClient returns the client's authenticated HTTP client or the fallback.
func authenticatedHTTPClient(client *xsapi.Client, fallback *http.Client) *http.Client {
	if client != nil {
		if httpClient := client.HTTPClient(); httpClient != nil {
			return httpClient
		}
	}
	return fallback
}

// broadcasterInviter resolves the current MPSD session dynamically for friend invites.
type broadcasterInviter struct {
	b *Broadcaster
}

// Invite snapshots the active MPSD session under the mutex and sends a game invite.
func (i *broadcasterInviter) Invite(ctx context.Context, xuid, titleID string) error {
	i.b.mu.Lock()
	announcer, ok := xblAnnouncer(i.b.announcer)
	if !ok || announcer.Session == nil {
		i.b.mu.Unlock()
		return errors.New("invite: no active MPSD session")
	}
	session := announcer.Session
	i.b.mu.Unlock()
	_, err := session.Invite(ctx, xuid, titleID)
	return err
}

// subAccountInviter sends friend invites through a sub-account's joined
// session, resolved dynamically so invites work as soon as the join lands.
type subAccountInviter struct {
	b  *Broadcaster
	id string
}

func (i *subAccountInviter) Invite(ctx context.Context, xuid, titleID string) error {
	i.b.mu.Lock()
	session := i.b.subSessionsByID[i.id]
	i.b.mu.Unlock()
	if session == nil {
		return errors.New("invite: sub-account has not joined the session")
	}
	_, err := session.Invite(ctx, xuid, titleID)
	return err
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
	switch a := announcer.(type) {
	case *room.XBLAnnouncer:
		return a, true
	case *sessionNonceAnnouncer:
		return a.XBLAnnouncer, true
	case loggingAnnouncer:
		return xblAnnouncer(a.Announcer)
	case signalingConnectionAnnouncer:
		return xblAnnouncer(a.Announcer)
	default:
		return nil, false
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
	mode, err := b.signalingMode()
	if err != nil {
		return nil, err
	}
	timeout := b.signalingDialTimeout()
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	b.debug("dialing nethernet signaling", "signaling_mode", mode, "timeout", timeout)
	conf := b.defaultSignalingConfig(mode)
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
func (b *Broadcaster) defaultSignalingConfig(mode SignalingMode) defaultSignalingConfig {
	client := b.conf.XBLClient
	if client == nil {
		client = b.xblClient
	}
	tokens := b.conf.MinecraftTokenSource
	if tokens == nil {
		tokens = b.minecraftTokens
	}
	return defaultSignalingConfig{
		mode:            mode,
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
	if conf.mode == SignalingModeJSONRPC {
		debugLog(conf.log, "dialing jsonrpc messaging signaling websocket")
		d := messaging.Dialer{
			Log:        conf.log,
			HTTPClient: conf.httpClient,
		}
		sig, err := d.DialContext(ctx, src)
		if err != nil {
			return defaultSignalingResult{createdClient: createdClient, err: err}
		}
		return defaultSignalingResult{signaling: sig, minecraft: src, createdClient: createdClient}
	}
	debugLog(conf.log, "dialing websocket signaling websocket")
	d := websocketsignaling.Dialer{
		Log:        conf.log,
		HTTPClient: conf.httpClient,
	}
	sig, err := d.DialContext(ctx, src)
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

// newAnnouncer creates and configures the MPSD session announcer.
func (b *Broadcaster) newAnnouncer(ctx context.Context) (room.Announcer, error) {
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
		Name:            b.conf.SessionName,
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
	}, b.primaryXUID(), b.log), nil
}

// startSubAccounts joins the primary session with all enabled sub-accounts.
// A failing sub-account is logged and skipped so it cannot take down the
// broadcaster, matching MCXboxBroadcast.
func (b *Broadcaster) startSubAccounts(ctx context.Context) error {
	for i := range b.conf.SubAccounts {
		account := &b.conf.SubAccounts[i]
		b.debug("checking sub-account",
			"sub_account", account.ID,
			"enabled", account.Enabled,
			"has_xbox_credentials", subAccountHasXBLCredentials(*account),
			"xuid", accountXUID(*account),
		)
		if !account.Enabled {
			continue
		}
		if !subAccountHasXBLCredentials(*account) {
			b.log.Warn("sub-account skipped because xbox live credentials are missing", "sub_account", account.ID)
			continue
		}
		if err := b.startSubAccountBounded(ctx, account); err != nil {
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

// startSubAccount prepares one sub-account and joins it to the primary session.
// startSubAccountBounded runs startSubAccount under the per-account timeout.
// The context only scopes the join requests; the sub-account's RTA connection
// and session outlive it.
func (b *Broadcaster) startSubAccountBounded(ctx context.Context, account *SubAccountConfig) error {
	timeout := b.subAccountStartTimeout
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return b.startSubAccount(ctx, account)
}

func (b *Broadcaster) startSubAccount(ctx context.Context, account *SubAccountConfig) error {
	if _, err := b.subAccountXBLClient(ctx, account); err != nil {
		return fmt.Errorf("prepare xbox live client: %w", err)
	}
	if account.XUID == "" {
		account.XUID = accountXUID(*account)
	}
	if account.XUID == "" {
		b.log.Warn("sub-account xuid unavailable", "sub_account", account.ID)
	}
	if err := b.ensureSubAccountMutualFollow(ctx, *account); err != nil {
		return fmt.Errorf("prepare mutual follow: %w", err)
	}
	pub := account.PublishConfig
	b.debug("joining sub-account to session",
		"sub_account", account.ID,
		"xuid", account.XUID,
		"session_name", b.sessionRef.Name,
	)
	s, err := b.joinSubAccount(ctx, *account, pub)
	if err != nil {
		return fmt.Errorf("join session: %w", err)
	}
	// b.mu is held by Start for the whole startup sequence, so the session
	// bookkeeping mutates directly; locking here would self-deadlock.
	b.subSessions = append(b.subSessions, s)
	if b.subSessionsByID == nil {
		b.subSessionsByID = make(map[string]*mpsd.Session)
	}
	b.subSessionsByID[account.ID] = s
	b.debug("joined sub-account to session", "sub_account", account.ID, "xuid", account.XUID)
	return nil
}

// joinSubAccount joins a sub-account to the primary session through the
// primary account's activity handle. Publishing the same session reference
// again would fail with 412 Precondition Failed, so the sub-account looks up
// the handle and joins it like MCXboxBroadcast's sub-sessions.
func (b *Broadcaster) joinSubAccount(ctx context.Context, account SubAccountConfig, pub mpsd.PublishConfig) (*mpsd.Session, error) {
	if b.subAccountPublisher != nil {
		return b.subAccountPublisher(ctx, account, b.sessionRef, pub)
	}
	if account.XBLClient == nil {
		return nil, errors.New("sub-account xbox live client is nil")
	}
	client := account.XBLClient.MPSD()
	if client == nil {
		return nil, errors.New("sub-account MPSD client is nil")
	}
	handleID, err := b.primaryActivityHandleID(ctx, client)
	if err != nil {
		return nil, err
	}
	b.debug("resolved primary activity handle", "sub_account", account.ID, "handle_id", handleID)
	s, err := client.Join(ctx, handleID, mpsd.JoinConfig{})
	if err != nil {
		return nil, fmt.Errorf("join handle %s: %w", handleID, err)
	}
	// Join publishes the sub-account's own activity handle as part of session
	// creation, which is what makes the session visible to the sub-account's
	// friends; no extra handle write is needed here.
	return s, nil
}

// primaryActivityHandleID finds the primary account's activity handle for the
// published session.
func (b *Broadcaster) primaryActivityHandleID(ctx context.Context, client *mpsd.Client) (uuid.UUID, error) {
	primaryXUID := b.primaryXUID()
	if primaryXUID == "" {
		return uuid.Nil, errors.New("primary xuid unavailable for activity handle lookup")
	}
	handles, err := client.ActivitiesForUsers(ctx, b.sessionRef.ServiceConfigID, []string{primaryXUID})
	if err != nil {
		return uuid.Nil, fmt.Errorf("query primary activity handles: %w", err)
	}
	for _, handle := range handles {
		if handle.SessionReference.Name == b.sessionRef.Name {
			return handle.ID, nil
		}
	}
	return uuid.Nil, fmt.Errorf("no activity handle found for session %q", b.sessionRef.Name)
}

// ensureSubAccountMutualFollow makes the primary and sub-account follow each
// other, skipping follows that already exist so restarts do not re-PUT both
// directions.
func (b *Broadcaster) ensureSubAccountMutualFollow(ctx context.Context, account SubAccountConfig) error {
	primaryXUID := b.primaryXUID()
	subXUID := accountXUID(account)
	if primaryXUID == "" || subXUID == "" || primaryXUID == subXUID {
		return nil
	}
	primary := b.friendClientFor(b.conf.XBLClient)
	sub := b.friendClientFor(account.XBLClient)
	primaryFollowsSub, subFollowsPrimary := subAccountFollowState(ctx, sub, primaryXUID)
	followed := false
	if !primaryFollowsSub {
		if err := primary.Follow(ctx, subXUID); err != nil {
			return fmt.Errorf("primary follow sub-account: %w", err)
		}
		followed = true
	}
	if !subFollowsPrimary {
		if err := sub.Follow(ctx, primaryXUID); err != nil {
			return fmt.Errorf("sub-account follow primary: %w", err)
		}
		followed = true
	}
	if followed && b.subAccountSettleDelay > 0 {
		// Give the friendship a moment to settle before the session join,
		// like MCXboxBroadcast's sub-session startup.
		select {
		case <-time.After(b.subAccountSettleDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// subAccountFollowState reports whether the primary and sub-account already
// follow each other, from the sub-account's view of the primary profile.
// Lookup failures return false so the follows are attempted anyway.
func subAccountFollowState(ctx context.Context, sub FriendClient, primaryXUID string) (primaryFollowsSub, subFollowsPrimary bool) {
	people, err := sub.Friends(ctx)
	if err != nil {
		return false, false
	}
	for _, p := range people {
		if p.XUID == primaryXUID {
			return p.IsFollowingCaller, p.IsFollowedByCaller
		}
	}
	return false, false
}

// uploadGallery uploads the configured showcase image to the Xbox gallery.
func (b *Broadcaster) uploadGallery(ctx context.Context) {
	cfg := b.conf.Gallery
	if cfg == nil || !cfg.Enabled {
		return
	}
	if _, err := os.Stat(cfg.ImagePath); errors.Is(err, os.ErrNotExist) {
		return
	}
	src := cfg.TokenSource
	if src == nil {
		tokens, err := b.minecraftTokenSource(b.sharedTokenSourceContext(ctx))
		if err != nil {
			b.log.Warn("minecraft services token source unavailable", "err", err)
			b.notify(ctx, "Showcase image upload skipped: Minecraft services token source is unavailable.")
			return
		}
		src = tokens
	}
	xuid := b.primaryXUID()
	if xuid == "" {
		b.log.Warn("gallery skipped because token XUID is empty")
		b.notify(ctx, "Showcase image upload skipped: Xbox profile XUID is empty.")
		return
	}
	client := GalleryClient{TokenSource: src, Client: cfg.Client, Log: b.log}
	if client.Client == nil {
		client.Client = b.conf.HTTPClient
	}
	b.info("setting showcase image", "path", cfg.ImagePath, "delete_other", cfg.DeleteOtherImages)
	result, err := client.SetShowcaseResult(ctx, xuid, cfg.ImagePath, cfg.DeleteOtherImages)
	if err != nil {
		b.log.Error("set showcase image", "err", err)
		b.notify(ctx, "Showcase image upload failed: "+err.Error())
		return
	}
	if result.AlreadySet {
		b.info("showcase image is already set, skipping upload", "image_id", result.ImageID)
	}
	b.info("successfully set showcase image", "path", cfg.ImagePath, "image_id", result.ImageID, "uploaded", result.Uploaded)
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

func (b *Broadcaster) debugEnabled() bool {
	return b.log != nil && b.log.Enabled(context.Background(), slog.LevelDebug)
}

func (b *Broadcaster) logMinecraftPacket(header packet.Header, payload []byte, src, dst net.Addr) {
	b.debug("minecraft packet",
		"packet_id", header.PacketID,
		"sender_sub_client", header.SenderSubClient,
		"target_sub_client", header.TargetSubClient,
		"payload_len", len(payload),
		"src", addrString(src),
		"dst", addrString(dst),
	)
}

func addrString(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	return addr.String()
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

// warnWebSocketSignalingMode warns when websocket signaling is configured instead of jsonrpc.
func (b *Broadcaster) warnWebSocketSignalingMode(mode SignalingMode) {
	if mode != SignalingModeWebSocket {
		return
	}
	b.warn(
		"websocket signaling may not appear in Minecraft friends list; use jsonrpc signaling for current Minecraft clients",
		"signaling_mode", mode,
		"recommended_signaling_mode", SignalingModeJSONRPC,
	)
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
func roomConnectionLogValues(connections []room.Connection) []roomConnectionLogValue {
	values := make([]roomConnectionLogValue, 0, len(connections))
	for _, connection := range connections {
		netherNetID := string(connection.NetherNetID)
		values = append(values, roomConnectionLogValue{
			ConnectionType: connection.ConnectionType,
			HostIPAddress:  connection.HostIPAddress,
			HostPort:       connection.HostPort,
			NetherNetID:    netherNetID,
			NetherNetIDSet: netherNetID != "" && netherNetID != "0",
			RakNetGUIDSet:  connection.RakNetGUID != "",
			PmsgID:         connection.PmsgID.String(),
			PmsgIDSet:      connection.PmsgID != uuid.Nil,
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

// uploadGalleryWithTimeout runs uploadGallery with a 30-second timeout.
func (b *Broadcaster) uploadGalleryWithTimeout() {
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	b.uploadGallery(ctx)
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

// acceptListener accepts connections from the given listener and transfers each to the target server.
func (b *Broadcaster) acceptListener(l *minecraft.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && b.ctx.Err() == nil {
				b.log.Error("accept client", "err", err)
			}
			return
		}
		mcConn, ok := conn.(*minecraft.Conn)
		if !ok {
			b.log.Error("accepted unexpected connection type", "type", fmt.Sprintf("%T", conn))
			_ = conn.Close()
			continue
		}
		go b.transfer(mcConn)
	}
}

// transfer sends a StartGame and Transfer packet to redirect a client to the target server.
func (b *Broadcaster) transfer(conn transferConn) {
	defer conn.Close()
	id := conn.IdentityData()
	if err := b.writeStartGameBeforeTransfer(conn); err != nil {
		b.log.Error("start game before transfer", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		return
	}
	if err := conn.WritePacket(&packet.Transfer{
		Address: b.conf.Server.Host,
		Port:    b.conf.Server.Port,
	}); err != nil {
		b.log.Error("transfer client", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		return
	}
	if err := conn.Flush(); err != nil {
		b.log.Error("flush transfer", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		return
	}
	if recorder, ok := b.conf.FriendHistory.(HistoryRecorder); ok && id.XUID != "" {
		if err := recorder.Seen(b.ctx, id.XUID, time.Now()); err != nil {
			b.log.Error("record player history", "xuid", id.XUID, "err", err)
		}
	}
	b.info("transferred bedrock client", "xuid", id.XUID, "name", id.DisplayName, "target", b.conf.Server.Address())
	b.waitForTransferredClientDisconnect(conn, id)
}

// effectiveTransferCloseTimeout returns the configured or default transfer close timeout.
func (b *Broadcaster) effectiveTransferCloseTimeout() time.Duration {
	if b.transferCloseTimeout == 0 {
		return defaultTransferCloseTimeout
	}
	if b.transferCloseTimeout < 0 {
		return 0
	}
	return b.transferCloseTimeout
}

// waitForTransferredClientDisconnect waits for a transferred client to disconnect or times out.
func (b *Broadcaster) waitForTransferredClientDisconnect(conn transferConn, id login.IdentityData) {
	timeout := b.effectiveTransferCloseTimeout()
	if timeout <= 0 {
		return
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		b.debug("set transfer disconnect deadline", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		return
	}
	cancelWait := b.closeTransferredClientOnStop(conn)
	defer cancelWait()

	b.debug("waiting for transferred client to disconnect", "xuid", id.XUID, "name", id.DisplayName, "timeout", timeout)
	for {
		if _, err := conn.ReadPacket(); err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded):
				b.debug("closing transferred client after disconnect timeout", "xuid", id.XUID, "name", id.DisplayName, "timeout", timeout)
			case b.ctx != nil && b.ctx.Err() != nil:
				b.debug("stopped waiting for transferred client disconnect", "xuid", id.XUID, "name", id.DisplayName, "err", err)
			default:
				b.debug("transferred client disconnected", "xuid", id.XUID, "name", id.DisplayName, "err", err)
			}
			return
		}
	}
}

// closeTransferredClientOnStop closes a transferred connection when the broadcaster stops.
func (b *Broadcaster) closeTransferredClientOnStop(conn transferConn) func() {
	if b.ctx == nil {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-b.ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	return func() {
		close(done)
	}
}

// writeStartGameBeforeTransfer writes the StartGame packet before transferring the client.
func (b *Broadcaster) writeStartGameBeforeTransfer(conn transferConn) error {
	pk := b.startGameBeforeTransfer()
	if err := conn.WritePacket(pk); err != nil {
		return fmt.Errorf("write StartGame: %w", err)
	}
	return nil
}

// startGameBeforeTransfer builds a minimal StartGame packet for the redirect flow.
func (b *Broadcaster) startGameBeforeTransfer() *packet.StartGame {
	worldName := b.conf.Status.WorldName
	if worldName == "" {
		worldName = b.conf.Status.HostName
	}
	if worldName == "" {
		worldName = b.conf.Server.Host
	}
	if worldName == "" {
		worldName = "Redirect"
	}
	return &packet.StartGame{
		EntityUniqueID:               1,
		EntityRuntimeID:              1,
		PlayerGameMode:               1,
		PlayerPosition:               mgl32.Vec3{0, 66, 0},
		Pitch:                        1,
		Yaw:                          1,
		WorldSeed:                    0,
		SpawnBiomeType:               packet.SpawnBiomeTypeDefault,
		UserDefinedBiomeName:         "",
		Dimension:                    2,
		Generator:                    1,
		WorldGameMode:                1,
		Difficulty:                   0,
		WorldSpawn:                   protocol.BlockPos{0, 0, 0},
		AchievementsDisabled:         true,
		MultiPlayerGame:              true,
		LANBroadcastEnabled:          true,
		XBLBroadcastMode:             packet.XBLBroadcastModePublic,
		PlatformBroadcastMode:        packet.XBLBroadcastModePublic,
		CommandsEnabled:              true,
		ChatRestrictionLevel:         packet.ChatRestrictionLevelNone,
		TexturePackRequired:          false,
		PlayerPermissions:            0,
		ServerChunkTickRadius:        4,
		HasLockedBehaviourPack:       false,
		HasLockedTexturePack:         false,
		FromLockedWorldTemplate:      false,
		MSAGamerTagsOnly:             false,
		FromWorldTemplate:            false,
		WorldTemplateSettingsLocked:  false,
		BaseGameVersion:              "*",
		LevelID:                      "",
		WorldName:                    worldName,
		TemplateContentIdentity:      "",
		Time:                         0,
		EnchantmentSeed:              0,
		MultiPlayerCorrelationID:     uuid.NewString(),
		ServerAuthoritativeInventory: false,
		GameVersion:                  "*",
		PropertyData:                 map[string]any{},
		WorldTemplateID:              uuid.Nil,
		ServerID:                     "",
		ScenarioID:                   "",
		WorldID:                      "",
		OwnerID:                      "",
		PlayerMovementSettings: protocol.PlayerMovementSettings{
			RewindHistorySize:                0,
			ServerAuthoritativeBlockBreaking: false,
		},
	}
}

const (
	// sessionMemberRestartThreshold mirrors Java's restart at 28/30 members;
	// a full member list permanently blocks new joiners.
	sessionMemberRestartThreshold = 28
	// sessionUpdateFailureLimit is how many consecutive update failures
	// trigger a full session recreation.
	sessionUpdateFailureLimit = 3
)

// updateLoop periodically refreshes the Xbox session metadata, checking
// session health before each update like Java's checkConnection().
func (b *Broadcaster) updateLoop() {
	ticker := time.NewTicker(b.conf.UpdateInterval)
	defer ticker.Stop()
	consecutiveFailures := 0
	for {
		select {
		case <-ticker.C:
			if b.checkSessionHealth() {
				consecutiveFailures = 0
				continue
			}
			ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
			if err := b.Update(ctx); err == nil {
				consecutiveFailures = 0
				b.debug("updated xbox live session")
			} else if !errors.Is(err, context.Canceled) {
				consecutiveFailures++
				b.log.Error("update session", "err", err)
				b.notifySessionUpdateFailure(ctx, err)
				if consecutiveFailures >= sessionUpdateFailureLimit {
					consecutiveFailures = 0
					b.recreateAfterFailure("repeated session update failures")
				}
			}
			cancel()
		case <-b.ctx.Done():
			return
		}
	}
}

// checkSessionHealth recreates the session when it is dead or nearly full and
// reports whether a recreation was attempted.
func (b *Broadcaster) checkSessionHealth() bool {
	reason := b.sessionUnhealthyReason()
	if reason == "" {
		return false
	}
	b.recreateAfterFailure(reason)
	return true
}

// sessionUnhealthyReason reports why the published session needs recreation,
// or an empty string when it is healthy.
func (b *Broadcaster) sessionUnhealthyReason() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	announcer, ok := xblAnnouncer(b.announcer)
	if !ok {
		return ""
	}
	announcer.Lock()
	session := announcer.Session
	announcer.Unlock()
	if session == nil {
		return ""
	}
	if session.Context().Err() != nil {
		return "mpsd session lost"
	}
	if count := sessionMemberCount(session); count >= sessionMemberRestartThreshold {
		return fmt.Sprintf("session has %d/30 members", count)
	}
	for _, s := range b.subSessions {
		if s.Context().Err() != nil {
			return "sub-account session lost"
		}
	}
	return ""
}

// sessionMemberCount counts the members of an MPSD session.
func sessionMemberCount(session *mpsd.Session) int {
	count := 0
	for range session.Members() {
		count++
	}
	return count
}

// recreateAfterFailure rebuilds the whole session stack after a health-check
// failure. Failures are logged and retried on the next tick rather than
// shutting the broadcaster down.
func (b *Broadcaster) recreateAfterFailure(reason string) {
	if b.ctx.Err() != nil {
		return
	}
	if !b.canRecreateSignaling() {
		b.warn("session is unhealthy but signaling is statically configured; cannot re-create", "reason", reason)
		return
	}
	b.warn("re-creating xbox live session", "reason", reason)
	if err := b.recreateSession(); err != nil {
		b.log.Error("re-create session failed", "reason", reason, "err", err)
		b.notify(b.ctx, "Xbox session recreation failed: "+err.Error())
		return
	}
	b.info("xbox live session re-created", "reason", reason)
}

// canRecreateSignaling reports whether signaling can be rebuilt (not statically configured).
func (b *Broadcaster) canRecreateSignaling() bool {
	return b.conf.Signaling == nil
}

// watchSignaling monitors the signaling context and triggers session reconnection on loss.
func (b *Broadcaster) watchSignaling() {
	sig := b.signaling
	if sig == nil {
		return
	}
	if !b.canRecreateSignaling() {
		b.debug("signaling reconnection disabled for static signaling")
		return
	}
	select {
	case <-sig.Context().Done():
		if b.ctx.Err() != nil {
			return
		}
		b.mu.Lock()
		current := b.signaling
		b.mu.Unlock()
		if current != nil && current != sig {
			// The session was already re-created (for example by the health
			// check); watch the replacement signaling instead.
			go b.watchSignaling()
			return
		}
		b.warn("connection to signaling lost, re-creating session...",
			"cause", context.Cause(sig.Context()))
		if err := b.recreateSession(); err != nil {
			b.log.Error("re-create session failed, shutting down", "err", err)
			b.notify(b.ctx, "Signaling reconnection failed, shutting down: "+err.Error())
			b.cancel()
			return
		}
		b.info("signaling session reconnected")
		go b.watchSignaling()
	case <-b.ctx.Done():
	}
}

// recreateSession tears down and rebuilds signaling, session, and listener after a drop.
func (b *Broadcaster) recreateSession() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.ctx.Err() != nil || !b.started {
		return errors.New("broadcaster is shut down")
	}

	b.acceptWg.Add(1)
	reconnectDone := false
	defer func() {
		if !reconnectDone {
			b.acceptWg.Done()
		}
	}()

	if b.listener != nil {
		_ = b.listener.Close()
	}
	_ = b.cleanupPublishedSessions(true)
	if b.signaling != nil {
		if c, ok := b.signaling.(interface{ Close() error }); ok {
			_ = c.Close()
		}
	}
	b.signaling = nil

	mode, err := b.signalingMode()
	if err != nil {
		return err
	}
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
	b.debug("nethernet signaling re-created", "signaling_mode", mode, "network_id", signalingNetworkID(sig))

	closeSignaling := func() {
		if c, ok := sig.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		b.signaling = nil
	}

	status, err := b.status(b.ctx)
	if err != nil {
		closeSignaling()
		return fmt.Errorf("re-create session status: %w", err)
	}
	announcer, err := b.newAnnouncer(b.ctx)
	if err != nil {
		closeSignaling()
		return fmt.Errorf("re-create announcer: %w", err)
	}
	b.announcer = loggingAnnouncer{Announcer: announcer, log: b.log}
	connection, err := b.signalingConnection(b.ctx, sig)
	if err != nil {
		closeSignaling()
		return fmt.Errorf("re-create signaling connection: %w", err)
	}
	if connection != nil {
		b.announcer = signalingConnectionAnnouncer{Announcer: b.announcer, connection: *connection}
	}
	if err := b.announcer.Announce(b.ctx, status); err != nil {
		closeSignaling()
		return fmt.Errorf("re-announce session: %w", err)
	}
	if err := b.startSubAccounts(b.ctx); err != nil {
		_ = b.cleanupPublishedSessions(true)
		closeSignaling()
		return fmt.Errorf("re-create sub-accounts: %w", err)
	}

	b.registerNetherNetNetwork(sig, status)

	listenConf := b.minecraftListenConfig(status)
	l, err := listenConf.Listen("nethernet", "")
	if err != nil {
		_ = b.cleanupPublishedSessions(true)
		closeSignaling()
		return fmt.Errorf("re-listen nethernet: %w", err)
	}
	b.listener = l
	b.info("nethernet broadcaster started", "network_id", signalingNetworkID(sig), "signaling_mode", mode)

	reconnectDone = true
	go func() {
		defer b.acceptWg.Done()
		b.acceptListener(l)
	}()
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
	status, err := b.status(ctx)
	if err != nil {
		return err
	}
	b.debugRoomStatus("resolved room status update", status)
	if err := b.announcer.Announce(ctx, status); err != nil {
		return err
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

// cleanupPublishedSessions closes sub-sessions and optionally the announcer.
func (b *Broadcaster) cleanupPublishedSessions(closeAnnouncer bool) error {
	var err error
	for _, s := range b.subSessions {
		err = errors.Join(err, s.Close())
	}
	b.subSessions = nil
	if closeAnnouncer && b.announcer != nil {
		err = errors.Join(err, b.announcer.Close())
	}
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
}

// xblClientCreated reports whether a client was created by the broadcaster.
func xblClientCreated(client *xsapi.Client, created map[*xsapi.Client]struct{}) bool {
	if client == nil {
		return false
	}
	_, ok := created[client]
	return ok
}

// Close stops the listener and removes the Xbox session.
func (b *Broadcaster) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started {
		return nil
	}
	b.cancel()
	err := b.listener.Close()
	err = errors.Join(err, b.cleanupPublishedSessions(false))
	if b.signaling != nil {
		if c, ok := b.signaling.(interface{ Close() error }); ok {
			err = errors.Join(err, c.Close())
		}
	}
	err = errors.Join(err, b.closeCreatedXBLClients())
	<-b.done
	b.started = false
	return err
}

// Wait blocks until the listener stops.
func (b *Broadcaster) Wait() {
	if b.done != nil {
		<-b.done
	}
}
