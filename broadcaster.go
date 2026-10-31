package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	announcer         room.Announcer
	retiredSessions   []retiredSession // rotated-out sessions, closed once drained
	relay             *httpRelay
	tokens            *tokenVerifier
	signaling         nethernet.Signaling
	sessionConnection *p2p.Connection
	staleAnnouncers   []room.Announcer // sessions whose close failed, retried on Update
	announcerFactory  func(*Broadcaster) room.Announcer
	// createdXBLClient is conf.XBLClient when the broadcaster created it, so Close also closes it.
	createdXBLClient       *xsapi.Client
	createdMinecraftTokens bool // conf.MinecraftTokenSource was created from conf.XBLClient

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

	health healthState

	// lastQuery is the most recent successful target-server query, kept so
	// query failures fall back to real data instead of failing the update.
	lastQuery *minecraft.ServerStatus

	galleryUploadTimeout time.Duration // zero uses 30 seconds
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
	if conf.XBLClient == nil && conf.XBLTokenSource == nil {
		return nil, errors.New("xbox live client or token source is required")
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
	b.debug("starting broadcaster",
		"status_provider", b.conf.StatusProvider != nil,
		"friend_sync", b.conf.FriendSync != nil,
		"gallery", b.conf.Gallery != nil && b.conf.Gallery.Enabled,
	)
	fail := func(err error) error {
		b.cancel()
		return errors.Join(err, b.releaseSessions(), b.closeCreatedXBLClient())
	}
	sig, err := b.signalingFor(b.ctx)
	if err != nil {
		return fail(err)
	}
	b.signaling = sig
	b.debug("nethernet signaling ready", "signaling_type", fmt.Sprintf("%T", sig), "network_id", signalingNetworkID(sig))
	b.info("creating xbox live session")
	if err := b.publishOn(b.ctx, sig); err != nil {
		return fail(err)
	}
	b.info("created xbox live session")
	b.started = true
	b.health.setIdentity(b.xuid(), clientGamertag(b.conf.XBLClient))
	b.info("nethernet broadcaster started", "network_id", signalingNetworkID(sig), "target", b.conf.JoinURL)

	go b.sessionLoop()
	if client, ok := b.presenceClient(); ok {
		b.debug("starting presence updates", "xuid", client.XUID)
		go client.Run(b.ctx, b.log)
	}
	go b.uploadGallery()
	if b.conf.FriendSync != nil && hasSocialClient(b.conf.XBLClient) {
		b.startFriendSync()
	}
	go b.logSocialSummary()
	return nil
}

// logSocialSummary logs the account and its friend usage, like MCXboxBroadcast's "N/2000 friends"
// line but against the 1000 limit. The social summary's targetFollowingCount is unreliable for the
// caller's own profile, so the count comes from the friend list.
func (b *Broadcaster) logSocialSummary() {
	if !hasSocialClient(b.conf.XBLClient) {
		return
	}
	ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
	defer cancel()
	friends, err := b.friendClient().Friends(ctx)
	if err != nil {
		b.debug("fetch friend list for summary", "err", err)
		return
	}
	b.info("authenticated to xbox live",
		"gamertag", b.hostNameFallback(),
		"xuid", b.xuid(),
		"friends", fmt.Sprintf("%d/%d", len(friends), MaxFriends),
	)
}

// presenceClient returns the Xbox presence client for heartbeat updates, if
// the account's Xbox client has one.
func (b *Broadcaster) presenceClient() (PresenceClient, bool) {
	if b.conf.XBLClient == nil || b.conf.XBLClient.Presence() == nil {
		return PresenceClient{}, false
	}
	return PresenceClient{XUID: b.xuid(), Presence: b.conf.XBLClient.Presence()}, true
}

// startFriendSync runs friend sync until the broadcaster stops. Only accepting requests acts on
// social events, so the RTA feed is subscribed only with AutoFriend.
func (b *Broadcaster) startFriendSync() {
	conf := b.conf.FriendSync
	b.debug("starting friend sync",
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
		syncer.Inviter = &broadcasterInviter{b: b}
	}
	if conf.AutoFriend {
		syncer.Trigger = b.subscribeSocial(b.conf.XBLClient.Social())
	}
	go syncer.Run(b.ctx)
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

// wrapAnnouncer applies logging and the shared signaling connection to a
// freshly created announcer.
func (b *Broadcaster) wrapAnnouncer(announcer room.Announcer) room.Announcer {
	announcer = loggingAnnouncer{Announcer: announcer, log: b.log}
	if b.sessionConnection != nil {
		announcer = signalingConnectionAnnouncer{Announcer: announcer, connection: *b.sessionConnection}
	}
	return announcer
}

// xblClient returns conf.XBLClient, creating it on first use.
func (b *Broadcaster) xblClient(ctx context.Context) (*xsapi.Client, error) {
	if b.conf.XBLClient != nil {
		return b.conf.XBLClient, nil
	}
	if b.conf.XBLTokenSource == nil {
		return nil, errors.New("xbox live token source is required")
	}
	client, err := NewXSAPIClient(ctx, b.conf.XBLTokenSource, b.conf.HTTPClient, b.log)
	if err != nil {
		return nil, fmt.Errorf("create xbox live client: %w", err)
	}
	b.conf.XBLClient, b.createdXBLClient = client, client
	return client, nil
}

// minecraftTokenSource returns conf.MinecraftTokenSource, creating it on first use. The source
// keeps ctx for later refreshes, so ctx must live as long as the broadcaster.
func (b *Broadcaster) minecraftTokenSource(ctx context.Context) (service.TokenSource, error) {
	if b.conf.MinecraftTokenSource != nil {
		return b.conf.MinecraftTokenSource, nil
	}
	client, err := b.xblClient(ctx)
	if err != nil {
		return nil, err
	}
	tokens, err := newMinecraftTokenSource(ctx, client, b.conf.HTTPClient, b.log)
	if err != nil {
		return nil, err
	}
	b.conf.MinecraftTokenSource, b.createdMinecraftTokens = tokens, true
	return tokens, nil
}

// xuid returns the account's XUID from config or client; empty before either knows it.
func (b *Broadcaster) xuid() string {
	if b.conf.XUID != "" {
		return b.conf.XUID
	}
	return clientXUID(b.conf.XBLClient)
}

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

func (b *Broadcaster) friendClient() FriendClient {
	return FriendClient{Social: b.conf.XBLClient.Social()}
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
	return defaultSignalingConfig{
		log:             b.log,
		httpClient:      b.conf.HTTPClient,
		xblClient:       b.conf.XBLClient,
		xblTokenSource:  b.conf.XBLTokenSource,
		minecraftTokens: b.conf.MinecraftTokenSource,
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
		b.conf.XBLClient, b.createdXBLClient = result.createdClient, result.createdClient
	}
	if b.conf.MinecraftTokenSource == nil && result.minecraft != nil {
		b.conf.MinecraftTokenSource, b.createdMinecraftTokens = result.minecraft, true
	}
}

// closeDefaultSignalingResult closes resources from a signaling dial result.
func closeDefaultSignalingResult(result defaultSignalingResult) {
	_ = closeSignaling(result.signaling)
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
	b.debug("using session signaling connection", "connection_type", connection.Type, "nethernet_id", connection.NetherNetID, "pmsg_id", connection.PlayerMessagingID)
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
	status, err := b.status(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve session status: %w", err)
	}
	announcer, err := b.newAnnouncer(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("create announcer: %w", err)
	}
	announcer = b.wrapAnnouncer(announcer)
	if err := announcer.Announce(ctx, status); err != nil {
		return nil, errors.Join(fmt.Errorf("announce session: %w", err), announcer.Close())
	}
	return announcer, nil
}

// newAnnouncer creates the MPSD session announcer; an empty name picks a random one.
func (b *Broadcaster) newAnnouncer(ctx context.Context, name string) (room.Announcer, error) {
	if b.announcerFactory != nil {
		b.debug("using configured xbox live announcer factory")
		return b.announcerFactory(b), nil
	}
	client, err := b.xblClient(ctx)
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
	pub := b.conf.PublishConfig
	b.debug("configured xbox live session",
		"service_config_id", ref.ServiceConfigID.String(),
		"template", ref.TemplateName,
		"session_name", ref.Name,
		"join_restriction", defaultString(pub.JoinRestriction, mpsd.SessionRestrictionFollowed),
		"read_restriction", defaultString(pub.ReadRestriction, mpsd.SessionRestrictionFollowed),
		"xuid", b.xuid(),
	)
	return newSessionNonceAnnouncer(&room.XBLAnnouncer{
		Client:           mpsdClient,
		SessionReference: ref,
		PublishConfig:    pub,
	}, b.xuid(), b.log, b.membersChanged), nil
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

// uploadGallery sets the configured showcase image of the account's gallery.
func (b *Broadcaster) uploadGallery() {
	b.galleryMu.Lock()
	defer b.galleryMu.Unlock()

	cfg := b.conf.Gallery
	if cfg == nil || !cfg.Enabled {
		return
	}
	if _, err := os.Stat(cfg.ImagePath); errors.Is(err, os.ErrNotExist) {
		return
	}
	ctx, cancel := b.galleryContext(b.ctx)
	defer cancel()
	src := cfg.TokenSource
	if src == nil {
		tokens, err := b.minecraftTokenSource(b.ctx)
		if err != nil {
			b.log.Warn("minecraft services token source unavailable", "err", err)
			b.notify(ctx, "Showcase image upload skipped: Minecraft services token source is unavailable.")
			return
		}
		src = tokens
	}
	xuid := b.xuid()
	if xuid == "" {
		b.log.Warn("gallery skipped because token XUID is empty")
		b.notify(ctx, "Showcase image upload skipped: Xbox profile XUID is empty.")
		return
	}
	client := GalleryClient{TokenSource: src, Client: cfg.Client, Log: b.log}
	if client.Client == nil {
		client.Client = b.conf.HTTPClient
	}
	args := []any{"path", cfg.ImagePath, "delete_other", cfg.DeleteOtherImages, "xuid", xuid}
	b.info("setting showcase image", args...)
	result, err := client.SetShowcaseResult(ctx, xuid, cfg.ImagePath, cfg.DeleteOtherImages)
	if err != nil {
		b.log.Error("set showcase image", append(args, "err", err)...)
		b.notify(ctx, "Showcase image upload failed: "+err.Error())
		return
	}
	if result.AlreadySet {
		b.info("showcase image is already set, skipping upload", append(args, "image_id", result.ImageID)...)
	}
	b.info("successfully set showcase image", append(args, "image_id", result.ImageID, "uploaded", result.Uploaded)...)
}

// galleryContext bounds one gallery upload.
func (b *Broadcaster) galleryContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	timeout := b.galleryUploadTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(parent, timeout)
}

func (b *Broadcaster) debug(msg string, args ...any) {
	debugLog(b.log, msg, args...)
}

func debugLog(log *slog.Logger, msg string, args ...any) {
	if log != nil {
		log.Debug(msg, args...)
	}
}

func (b *Broadcaster) info(msg string, args ...any) {
	if b.log != nil {
		b.log.Info(msg, args...)
	}
}

func (b *Broadcaster) warn(msg string, args ...any) {
	if b.log != nil {
		b.log.Warn(msg, args...)
	}
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

// signalingNetworkID tolerates the nil signaling a SignalingFactory may return.
func signalingNetworkID(sig nethernet.Signaling) string {
	if sig == nil {
		return ""
	}
	return sig.NetworkID()
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

const (
	sessionMemberCap = 30 // members the MinecraftLobby template allows
	// sessionRotateMembers rotates before sessionMemberCap so joins in
	// progress still fit; players stay members while they play.
	sessionRotateMembers = 28
	// sessionRotateGrace keeps a rotated-out session joinable for players mid-join.
	sessionRotateGrace = 30 * time.Second
	// sessionUpdateFailureLimit is how many consecutive update failures
	// trigger a full session recreation.
	sessionUpdateFailureLimit = 3
)

type sessionHealthIssue struct {
	reason   string
	activity *publishedActivity
	rotate   bool // the session is nearly full rather than broken
}

// sessionHealthIssue reports why the current session needs rotation or
// recovery, or an empty issue when it is healthy.
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
		return sessionHealthIssue{reason: fmt.Sprintf("session has %d/%d members", count, sessionMemberCap), rotate: true}
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

// rotateSession moves the activity to a fresh session on the same signaling
// connection and retires the old one, like the Java rotation.
func (b *Broadcaster) rotateSession(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started || b.recovering {
		return errors.New("broadcaster is not running")
	}
	announcer, err := b.publishSession(ctx, "")
	if err != nil {
		return err
	}
	b.retire(b.announcer)
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
func (b *Broadcaster) retire(announcer room.Announcer) {
	b.retiredSessions = append(b.retiredSessions, retiredSession{announcer: announcer, at: time.Now()})
	if !b.conf.HideOldSessions {
		return
	}
	hider, ok := unwrapAnnouncer(announcer).(sessionHider)
	if !ok {
		return
	}
	ctx, log := b.ctx, b.log
	go func() {
		select {
		case <-time.After(sessionRotateGrace):
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
	kept := b.retiredSessions[:0]
	for _, retired := range b.retiredSessions {
		counter, ok := unwrapAnnouncer(retired.announcer).(memberCounter)
		count, published := 0, false
		if ok {
			count, published = counter.memberCount()
		}
		if !published || count > 1 || time.Since(retired.at) < sessionRotateGrace {
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
	_ = b.releaseSessions()

	sig, err := b.signalingFor(b.ctx)
	if err != nil {
		return fmt.Errorf("re-create signaling: %w", err)
	}
	if sig.Context().Err() != nil {
		_ = closeSignaling(sig)
		return errors.New("re-create signaling: factory returned signaling with dead context")
	}
	b.signaling = sig
	b.debug("nethernet signaling re-created", "network_id", signalingNetworkID(sig))

	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	if err := b.publishOn(ctx, sig); err != nil {
		_ = closeSignaling(sig)
		b.signaling = nil
		return fmt.Errorf("re-create session: %w", err)
	}
	b.debug("join relay restarted", "network_id", signalingNetworkID(sig), "target", b.conf.JoinURL)

	// Re-showcase the gallery image so a swapped file does not require a
	// process restart; the upload no-ops when the image is already set.
	go b.uploadGallery()
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
	if b.announcer == nil {
		return errors.New("no published session")
	}
	status, err := b.status(ctx)
	if err != nil {
		return err
	}
	if err := b.announcer.Announce(ctx, status); err != nil {
		return err
	}
	b.closeStaleSessions()
	// Matching MCXboxBroadcast, suppressSessionUpdateMessage only demotes the
	// periodic success log to debug level.
	if b.conf.SuppressSessionUpdateMessage {
		b.debug("updated session")
	} else {
		b.info("updated session")
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
			b.warn("retry close stale session", "err", err)
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
		err = errors.Join(err, b.announcer.Close())
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

// closeCreatedXBLClient closes the Xbox Live client the broadcaster created and forgets it,
// with the token source derived from it, so a later Start creates fresh ones.
func (b *Broadcaster) closeCreatedXBLClient() error {
	client := b.createdXBLClient
	if client == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	err := client.CloseContext(ctx)

	// A running gallery upload still uses the client's token source.
	b.galleryMu.Lock()
	defer b.galleryMu.Unlock()
	b.conf.XBLClient, b.createdXBLClient = nil, nil
	if b.createdMinecraftTokens {
		b.conf.MinecraftTokenSource, b.createdMinecraftTokens = nil, false
	}
	return err
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
	err := errors.Join(b.releaseSessions(), b.closeCreatedXBLClient())
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
