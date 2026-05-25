package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi"
	"github.com/df-mc/go-xsapi/mpsd"
	"github.com/go-gl/mathgl/mgl32"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service/signaling"
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
	announcerFactory    func(*Broadcaster) room.Announcer
	subAccountPublisher func(context.Context, SubAccountConfig, mpsd.SessionReference, mpsd.PublishConfig) (*mpsd.Session, error)

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	started bool
}

type transferConn interface {
	WritePacket(packet.Packet) error
	Flush() error
	Close() error
	IdentityData() login.IdentityData
}

// New validates conf and returns a Broadcaster.
func New(conf Config) (*Broadcaster, error) {
	if err := conf.Server.validate(); err != nil {
		return nil, err
	}
	if conf.TokenSource == nil {
		return nil, errors.New("token source is required")
	}
	if conf.LiveTokenSource == nil && conf.Signaling == nil && conf.SignalingFactory == nil {
		return nil, errors.New("live token source, signaling, or signaling factory is required")
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
	return &Broadcaster{conf: conf, log: conf.Log.With("src", "broadcaster")}, nil
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

	sig, err := b.signalingFor(b.ctx)
	if err != nil {
		b.cancel()
		return err
	}
	b.signaling = sig

	status, err := b.status(b.ctx)
	if err != nil {
		b.cancel()
		return err
	}
	b.announcer = b.newAnnouncer()
	if err := b.announcer.Announce(b.ctx, status); err != nil {
		b.cancel()
		err = errors.Join(fmt.Errorf("announce session: %w", err), b.cleanupStartupFailure(true))
		return err
	}
	if err := b.startSubAccounts(b.ctx); err != nil {
		b.cancel()
		err = errors.Join(err, b.cleanupStartupFailure(true))
		return err
	}

	minecraft.RegisterNetwork("nethernet", func(l *slog.Logger) minecraft.Network {
		return room.Network{
			Network: minecraft.NetherNet{
				Signaling: sig,
				ListenConfig: nethernet.ListenConfig{
					Log:                b.log,
					ConnContext:        b.conf.NetherNetListenConfig.ConnContext,
					NegotiationContext: b.conf.NetherNetListenConfig.NegotiationContext,
					ICEGatherPolicy:    b.conf.NetherNetListenConfig.ICEGatherPolicy,
					DisableTrickleICE:  b.conf.NetherNetListenConfig.DisableTrickleICE,
					API:                b.conf.NetherNetListenConfig.API,
				},
			},
			ListenConfig: b.roomListenConfig(status),
		}
	})

	listenConf := b.conf.ListenConfig
	listenConf.ErrorLog = b.log
	listenConf.StatusProvider = b.minecraftStatusProvider(status)
	listenConf.AuthenticationDisabled = true
	l, err := listenConf.Listen("nethernet", "")
	if err != nil {
		b.cancel()
		err = errors.Join(fmt.Errorf("listen nethernet: %w", err), b.cleanupStartupFailure(true))
		return err
	}
	b.listener = l
	b.started = true

	go b.accept()
	go b.updateLoop()
	for _, client := range b.presenceClients() {
		go client.Run(b.ctx, b.log)
	}
	go b.uploadGalleryWithTimeout()
	if b.conf.FriendSync != nil {
		go b.friendSyncer().Run(b.ctx)
	}
	return nil
}

func (b *Broadcaster) presenceClients() []PresenceClient {
	clients := []PresenceClient{{TokenSource: b.conf.TokenSource, Client: b.conf.HTTPClient}}
	for _, account := range b.conf.SubAccounts {
		if !account.Enabled || account.TokenSource == nil {
			continue
		}
		clients = append(clients, PresenceClient{TokenSource: account.TokenSource, Client: b.conf.HTTPClient})
	}
	return clients
}

func (b *Broadcaster) friendSyncer() FriendSyncer {
	syncer := FriendSyncer{
		Client:  FriendClient{TokenSource: b.conf.TokenSource, Client: b.conf.HTTPClient},
		Config:  *b.conf.FriendSync,
		History: b.conf.FriendHistory,
		Log:     b.log,
	}
	if announcer, ok := b.announcer.(*room.XBLAnnouncer); ok && announcer.Session != nil {
		syncer.Inviter = sessionInviter{session: announcer.Session}
	}
	return syncer
}

func (b *Broadcaster) roomStatusProvider(status room.Status) room.StatusProvider {
	if b.conf.StatusProvider != nil {
		return normalizedStatusProvider{Provider: b.conf.StatusProvider}
	}
	return room.NewStatusProvider(status)
}

func (b *Broadcaster) roomListenConfig(status room.Status) room.ListenConfig {
	return room.ListenConfig{
		Announcer:      b.announcer,
		StatusProvider: b.roomStatusProvider(status),
		Log:            b.log,
	}
}

func (b *Broadcaster) minecraftStatusProvider(status room.Status) minecraft.ServerStatusProvider {
	if b.conf.StatusProvider != nil {
		return roomMinecraftStatusProvider{Provider: normalizedStatusProvider{Provider: b.conf.StatusProvider}}
	}
	return minecraft.NewStatusProvider(status.WorldName, status.HostName)
}

func (b *Broadcaster) signalingFor(ctx context.Context) (nethernet.Signaling, error) {
	if b.conf.Signaling != nil {
		return b.conf.Signaling, nil
	}
	if b.conf.SignalingFactory != nil {
		return b.conf.SignalingFactory(ctx, b.conf)
	}
	d := signaling.Dialer{
		Log:        b.log,
		HTTPClient: b.conf.HTTPClient,
	}
	return d.DialContext(ctx, b.conf.LiveTokenSource)
}

func (b *Broadcaster) newAnnouncer() room.Announcer {
	if b.announcerFactory != nil {
		return b.announcerFactory(b)
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
	pub.Logger = b.log
	return &room.XBLAnnouncer{
		TokenSource:      b.conf.TokenSource,
		SessionReference: ref,
		PublishConfig:    pub,
	}
}

func (b *Broadcaster) startSubAccounts(ctx context.Context) error {
	for _, account := range b.conf.SubAccounts {
		if !account.Enabled || account.TokenSource == nil {
			continue
		}
		if err := b.ensureSubAccountMutualFollow(ctx, account); err != nil {
			return fmt.Errorf("prepare sub-account %q mutual follow: %w", account.ID, err)
		}
		pub := account.PublishConfig
		pub.Logger = b.log.With("sub_account", account.ID)
		s, err := b.publishSubAccount(ctx, account, pub)
		if err != nil {
			return fmt.Errorf("start sub-account %q: %w", account.ID, err)
		}
		b.subSessions = append(b.subSessions, s)
	}
	return nil
}

func (b *Broadcaster) publishSubAccount(ctx context.Context, account SubAccountConfig, pub mpsd.PublishConfig) (*mpsd.Session, error) {
	if b.subAccountPublisher != nil {
		return b.subAccountPublisher(ctx, account, b.sessionRef, pub)
	}
	return pub.PublishContext(ctx, account.TokenSource, b.sessionRef)
}

func (b *Broadcaster) ensureSubAccountMutualFollow(ctx context.Context, account SubAccountConfig) error {
	primaryXUID, primaryOK, err := tokenSourceXUID(b.conf.TokenSource)
	if err != nil {
		return fmt.Errorf("primary xuid: %w", err)
	}
	subXUID, subOK, err := tokenSourceXUID(account.TokenSource)
	if err != nil {
		return fmt.Errorf("sub-account xuid: %w", err)
	}
	if !primaryOK || !subOK || primaryXUID == subXUID {
		return nil
	}
	primary := FriendClient{TokenSource: b.conf.TokenSource, Client: b.conf.HTTPClient}
	if err := primary.Follow(ctx, subXUID); err != nil {
		return fmt.Errorf("primary follow sub-account: %w", err)
	}
	sub := FriendClient{TokenSource: account.TokenSource, Client: b.conf.HTTPClient}
	if err := sub.Follow(ctx, primaryXUID); err != nil {
		return fmt.Errorf("sub-account follow primary: %w", err)
	}
	return nil
}

func tokenSourceXUID(src xsapi.TokenSource) (string, bool, error) {
	if src == nil {
		return "", false, nil
	}
	tok, err := src.Token()
	if err != nil {
		return "", false, err
	}
	xuid := tok.DisplayClaims().XUID
	if xuid == "" {
		return "", false, nil
	}
	return xuid, true, nil
}

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
		src = b.conf.MinecraftTokenSource
	}
	if src == nil && b.conf.LiveTokenSource != nil {
		tokens, err := newMinecraftTokenSource(ctx, ctx, b.conf.LiveTokenSource, b.conf.HTTPClient)
		if err != nil {
			b.log.Warn("minecraft services token source unavailable", "err", err)
			b.notify(ctx, "Showcase image upload skipped: Minecraft services token source is unavailable.")
			return
		}
		src = tokens
	}
	if src == nil {
		b.log.Warn("gallery enabled without minecraft token source")
		b.notify(ctx, "Showcase image upload skipped: Minecraft token source is not configured.")
		return
	}
	tok, err := b.conf.TokenSource.Token()
	if err != nil {
		b.log.Error("gallery xuid token", "err", err)
		b.notify(ctx, "Showcase image upload failed while reading the Xbox profile: "+err.Error())
		return
	}
	xuid := tok.DisplayClaims().XUID
	if xuid == "" {
		b.log.Warn("gallery skipped because token XUID is empty")
		b.notify(ctx, "Showcase image upload skipped: Xbox profile XUID is empty.")
		return
	}
	client := GalleryClient{TokenSource: src, Client: cfg.Client}
	if client.Client == nil {
		client.Client = b.conf.HTTPClient
	}
	if err := client.SetShowcase(ctx, xuid, cfg.ImagePath, cfg.DeleteOtherImages); err != nil {
		b.log.Error("set showcase image", "err", err)
		b.notify(ctx, "Showcase image upload failed: "+err.Error())
	}
}

func (b *Broadcaster) uploadGalleryWithTimeout() {
	ctx, cancel := context.WithTimeout(b.ctx, 30*time.Second)
	defer cancel()
	b.uploadGallery(ctx)
}

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

func (b *Broadcaster) notifySessionUpdateFailure(ctx context.Context, err error) {
	if b.conf.SuppressSessionUpdateMessage {
		return
	}
	b.notify(ctx, "Xbox session update failed: "+err.Error())
}

func (b *Broadcaster) accept() {
	defer close(b.done)
	for {
		conn, err := b.listener.Accept()
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
	_ = conn.Flush()
	if recorder, ok := b.conf.FriendHistory.(HistoryRecorder); ok && id.XUID != "" {
		if err := recorder.Seen(b.ctx, id.XUID, time.Now()); err != nil {
			b.log.Error("record player history", "xuid", id.XUID, "err", err)
		}
	}
	b.log.Info("transferred client", "xuid", id.XUID, "name", id.DisplayName, "target", b.conf.Server.Address())
}

func (b *Broadcaster) writeStartGameBeforeTransfer(conn transferConn) error {
	pk := b.startGameBeforeTransfer()
	if err := conn.WritePacket(pk); err != nil {
		return fmt.Errorf("write StartGame: %w", err)
	}
	return nil
}

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
		PlayerGameMode:               2,
		PlayerPosition:               mgl32.Vec3{0, 64, 0},
		WorldSeed:                    0,
		SpawnBiomeType:               packet.SpawnBiomeTypeDefault,
		Dimension:                    0,
		Generator:                    1,
		WorldGameMode:                2,
		Difficulty:                   1,
		WorldSpawn:                   protocol.BlockPos{0, 64, 0},
		AchievementsDisabled:         true,
		MultiPlayerGame:              true,
		LANBroadcastEnabled:          true,
		XBLBroadcastMode:             packet.XBLBroadcastModeFriendsOfFriends,
		PlatformBroadcastMode:        packet.XBLBroadcastModeFriendsOfFriends,
		CommandsEnabled:              true,
		PlayerPermissions:            1,
		ServerChunkTickRadius:        4,
		WorldTemplateSettingsLocked:  true,
		BaseGameVersion:              protocol.CurrentVersion,
		LevelID:                      "broadcaster_redirect",
		WorldName:                    worldName,
		MultiPlayerCorrelationID:     uuid.NewString(),
		ServerAuthoritativeInventory: true,
		GameVersion:                  protocol.CurrentVersion,
		PropertyData:                 map[string]any{},
		PlayerMovementSettings: protocol.PlayerMovementSettings{
			ServerAuthoritativeBlockBreaking: true,
		},
	}
}

func (b *Broadcaster) updateLoop() {
	ticker := time.NewTicker(b.conf.UpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(b.ctx, 15*time.Second)
			if err := b.Update(ctx); err != nil && !errors.Is(err, context.Canceled) {
				b.log.Error("update session", "err", err)
				b.notifySessionUpdateFailure(ctx, err)
			}
			cancel()
		case <-b.ctx.Done():
			return
		}
	}
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
	return b.announcer.Announce(ctx, status)
}

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

func (b *Broadcaster) cleanupStartupFailure(closeAnnouncer bool) error {
	err := b.cleanupPublishedSessions(closeAnnouncer)
	if b.signaling != nil {
		if c, ok := b.signaling.(interface{ Close() error }); ok {
			err = errors.Join(err, c.Close())
		}
	}
	return err
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
