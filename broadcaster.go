package broadcaster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
	"github.com/sandertv/gophertunnel/minecraft/room"
	"github.com/sandertv/gophertunnel/minecraft/service/signaling"
)

// Broadcaster owns the Xbox Live session, NetherNet listener, and redirect
// loop for a published Bedrock server.
type Broadcaster struct {
	conf Config
	log  *slog.Logger

	announcer *room.XBLAnnouncer
	listener  *minecraft.Listener
	signaling nethernet.Signaling

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu      sync.Mutex
	started bool
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
		return fmt.Errorf("announce session: %w", err)
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
			ListenConfig: room.ListenConfig{
				Announcer:      b.announcer,
				StatusProvider: room.NewStatusProvider(status),
				Log:            b.log,
			},
		}
	})

	listenConf := b.conf.ListenConfig
	listenConf.ErrorLog = b.log
	listenConf.StatusProvider = minecraft.NewStatusProvider(status.WorldName, status.HostName)
	listenConf.AuthenticationDisabled = true
	l, err := listenConf.Listen("nethernet", "")
	if err != nil {
		b.cancel()
		return fmt.Errorf("listen nethernet: %w", err)
	}
	b.listener = l
	b.started = true

	go b.accept()
	go b.updateLoop()
	return nil
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

func (b *Broadcaster) newAnnouncer() *room.XBLAnnouncer {
	ref := mpsd.SessionReference{
		ServiceConfigID: serviceConfigUUID,
		TemplateName:    TemplateName,
		Name:            b.conf.SessionName,
	}
	if ref.Name == "" {
		ref.Name = strings.ToUpper(uuid.NewString())
	}
	pub := b.conf.PublishConfig
	pub.Logger = b.log
	return &room.XBLAnnouncer{
		TokenSource:      b.conf.TokenSource,
		SessionReference: ref,
		PublishConfig:    pub,
	}
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

func (b *Broadcaster) transfer(conn *minecraft.Conn) {
	defer conn.Close()
	id := conn.IdentityData()
	if err := conn.WritePacket(&packet.Transfer{
		Address: b.conf.Server.Host,
		Port:    b.conf.Server.Port,
	}); err != nil {
		b.log.Error("transfer client", "xuid", id.XUID, "name", id.DisplayName, "err", err)
		return
	}
	_ = conn.Flush()
	b.log.Info("transferred client", "xuid", id.XUID, "name", id.DisplayName, "target", b.conf.Server.Address())
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

// Close stops the listener and removes the Xbox session.
func (b *Broadcaster) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started {
		return nil
	}
	b.cancel()
	err := b.listener.Close()
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
