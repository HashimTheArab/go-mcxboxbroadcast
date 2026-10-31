package broadcaster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

// newAnnouncer creates the MPSD session announcer; an empty name picks a random one.
func (b *Broadcaster) newAnnouncer(name string) room.Announcer {
	if b.announcerFactory != nil {
		return b.announcerFactory(b)
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
	b.log.Debug("configured xbox live session",
		"service_config_id", ref.ServiceConfigID.String(),
		"template", ref.TemplateName,
		"session_name", ref.Name,
		"join_restriction", defaultString(pub.JoinRestriction, mpsd.SessionRestrictionFollowed),
		"read_restriction", defaultString(pub.ReadRestriction, mpsd.SessionRestrictionFollowed),
		"xuid", b.xuid(),
	)
	return newSessionNonceAnnouncer(&room.XBLAnnouncer{
		Client:           b.conf.XBLClient.MPSD(),
		SessionReference: ref,
		PublishConfig:    pub,
	}, b.xuid(), b.log, b.membersChanged)
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

type signalingConnectionAnnouncer struct {
	room.Announcer
	connection p2p.Connection
}

func (a signalingConnectionAnnouncer) Announce(ctx context.Context, status room.Status) error {
	// The session document must advertise exactly the shared signaling
	// connection clients can join through; any caller-provided connections are
	// deliberately replaced.
	status.SupportedConnections = []p2p.Connection{a.connection}
	return a.Announcer.Announce(ctx, status)
}

// signalingConnection builds the MPSD connection entry that advertises sig's
// JSON-RPC identity.
func (b *Broadcaster) signalingConnection(sig nethernet.Signaling) (*p2p.Connection, error) {
	if sig == nil {
		return nil, errors.New("signaling connection: signaling is nil")
	}
	if strings.TrimSpace(sig.NetworkID()) == "" {
		return nil, errors.New("signaling connection: nethernet id is empty")
	}
	jsonRPCSignaling, ok := sig.(p2p.JSONRPCSignaling)
	if !ok {
		return nil, fmt.Errorf("signaling %T does not expose its player messaging identity", sig)
	}
	connection, err := p2p.NewJSONRPCConnection(jsonRPCSignaling)
	if err != nil {
		return nil, fmt.Errorf("validate signaling connection: %w", err)
	}
	return &connection, nil
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

// sessionHider is implemented by announcers whose session can be made invite-only.
type sessionHider interface {
	hide(ctx context.Context) error
}

// memberCounter is implemented by announcers that publish an MPSD session.
type memberCounter interface {
	// memberCount is zero for a lost session, whose MPSD context has ended.
	memberCount() (count int, published, lost bool)
}

type sessionNonceAnnouncer struct {
	*room.XBLAnnouncer

	ownerXUID string
	log       *slog.Logger
	// membersChanged receives the member count after each session change.
	membersChanged func(count int)

	custom          []byte
	readRestriction string
	joinRestriction string
	nonces          map[string]string
	lastStatus      room.Status
	handledSession  *mpsd.Session
}

func newSessionNonceAnnouncer(announcer *room.XBLAnnouncer, ownerXUID string, log *slog.Logger, membersChanged func(int)) *sessionNonceAnnouncer {
	return &sessionNonceAnnouncer{
		XBLAnnouncer:   announcer,
		ownerXUID:      ownerXUID,
		log:            log,
		membersChanged: membersChanged,
		nonces:         make(map[string]string),
	}
}

func (a *sessionNonceAnnouncer) memberCount() (count int, published, lost bool) {
	a.Lock()
	session := a.Session
	a.Unlock()
	if session == nil {
		return 0, false, false
	}
	if session.Context().Err() != nil {
		return 0, true, true
	}
	return sessionMemberCount(session), true, false
}

// hide makes the published session invite-only so its members no longer
// advertise it to their friends. It keeps the session's restrictions, since
// changing them would republish it.
func (a *sessionNonceAnnouncer) hide(ctx context.Context) error {
	a.Lock()
	defer a.Unlock()
	if a.Session == nil {
		return errors.New("session is not published")
	}
	a.lastStatus.BroadcastSetting = p2p.BroadcastSettingInviteOnly
	a.lastStatus.Joinability = p2p.JoinabilityInviteOnly
	custom, err := marshalStatusWithNonces(a.lastStatus, a.nonces)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if err := a.Session.SetCustomProperties(ctx, custom); err != nil {
		return fmt.Errorf("set custom properties: %w", err)
	}
	a.custom = custom
	return nil
}

func (a *sessionNonceAnnouncer) Announce(ctx context.Context, status room.Status) error {
	a.Lock()
	defer a.Unlock()

	read, join := a.publishRestrictions(status)
	if a.Session != nil && a.readRestriction == "" && a.joinRestriction == "" {
		if properties := a.Session.Properties(); properties.System != nil {
			a.readRestriction = properties.System.ReadRestriction
			a.joinRestriction = properties.System.JoinRestriction
		}
	}

	custom, err := marshalStatusWithNonces(status, a.nonces)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	a.lastStatus = status
	if a.Session != nil && bytes.Equal(custom, a.custom) && read == a.readRestriction && join == a.joinRestriction {
		a.handleSessionLocked()
		return nil
	}

	restrictionsChanged := read != a.readRestriction || join != a.joinRestriction
	if a.Session != nil && restrictionsChanged {
		if a.Client == nil {
			return errors.New("republish session: mpsd client is nil")
		}
		if err := a.Session.CloseContext(ctx); err != nil {
			return fmt.Errorf("close stale session: %w", err)
		}
		a.resetForRepublishLocked()
	}

	custom, err = marshalStatusWithNonces(status, a.nonces)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	config, read, join := a.publishConfig(status, custom)

	if a.Session == nil {
		if a.Client == nil {
			return errors.New("publish session: mpsd client is nil")
		}
		session, err := a.Client.Publish(ctx, a.SessionReference, config)
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		a.Session = session
	} else if err := a.Session.SetCustomProperties(ctx, custom); err != nil {
		return fmt.Errorf("set custom properties: %w", err)
	}

	a.custom = custom
	a.readRestriction = read
	a.joinRestriction = join
	a.handleSessionLocked()
	a.log.Debug("published mpsd session nonces", "nonce_count", len(a.nonces))
	return nil
}

func (a *sessionNonceAnnouncer) resetForRepublishLocked() {
	a.Session = nil
	a.handledSession = nil
	a.custom = nil
	a.readRestriction = ""
	a.joinRestriction = ""
	a.nonces = make(map[string]string)
}

func (a *sessionNonceAnnouncer) publishConfig(status room.Status, custom []byte) (mpsd.PublishConfig, string, string) {
	read, join := a.publishRestrictions(status)
	config := a.PublishConfig
	config.CustomProperties = custom
	config.ReadRestriction = read
	config.JoinRestriction = join
	return config, read, join
}

func (a *sessionNonceAnnouncer) publishRestrictions(status room.Status) (read, join string) {
	setting := status.BroadcastSetting
	if !setting.Valid() {
		setting = p2p.BroadcastSettingFriendsOfFriends
	}
	read, join = setting.ReadRestriction(), setting.JoinRestriction()
	if a.PublishConfig.ReadRestriction != "" {
		read = a.PublishConfig.ReadRestriction
	}
	if a.PublishConfig.JoinRestriction != "" {
		join = a.PublishConfig.JoinRestriction
	}
	return read, join
}

func (a *sessionNonceAnnouncer) handleSessionLocked() {
	if a.Session == nil || a.Session == a.handledSession {
		return
	}
	a.handledSession = a.Session
	a.Session.Handle(sessionNonceHandler{announcer: a})
}

func (a *sessionNonceAnnouncer) updateNoncesFromSession(ctx context.Context, session *mpsd.Session) error {
	return a.updateNonces(ctx, session, sessionMemberXUIDs(session), func(ctx context.Context, custom json.RawMessage) error {
		return a.Session.SetCustomProperties(ctx, custom)
	})
}

func (a *sessionNonceAnnouncer) updateNonces(ctx context.Context, session *mpsd.Session, activeXUIDs []string, setCustomProperties func(context.Context, json.RawMessage) error) error {
	a.Lock()
	defer a.Unlock()
	if session != a.Session {
		return nil
	}
	changed, err := syncSessionNonces(a.nonces, activeXUIDs, a.ownerXUID, generateSessionNonce)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	custom, err := marshalStatusWithNonces(a.lastStatus, a.nonces)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if err := setCustomProperties(ctx, custom); err != nil {
		return fmt.Errorf("set custom properties: %w", err)
	}
	a.custom = custom
	a.log.Debug("updated mpsd session nonces", "nonce_count", len(a.nonces), "active_member_count", len(activeXUIDs))
	return nil
}

type sessionNonceHandler struct {
	announcer *sessionNonceAnnouncer
}

func (h sessionNonceHandler) HandleSessionChange(session *mpsd.Session) {
	h.announcer.membersChanged(sessionMemberCount(session))
	ctx, cancel := context.WithTimeout(session.Context(), 15*time.Second)
	defer cancel()
	if err := h.announcer.updateNoncesFromSession(ctx, session); err != nil {
		h.announcer.log.Error("update mpsd session nonces", "err", err)
	}
}

func sessionMemberXUIDs(session *mpsd.Session) []string {
	var xuids []string
	for _, member := range session.Members() {
		if member.Constants == nil || member.Constants.System == nil {
			continue
		}
		xuid := strings.TrimSpace(member.Constants.System.XUID)
		if xuid == "" {
			continue
		}
		xuids = append(xuids, xuid)
	}
	return xuids
}

func syncSessionNonces(nonces map[string]string, activeXUIDs []string, ownerXUID string, generate func() (string, error)) (bool, error) {
	ownerXUID = strings.TrimSpace(ownerXUID)
	active := make(map[string]struct{}, len(activeXUIDs))
	for _, xuid := range activeXUIDs {
		xuid = strings.TrimSpace(xuid)
		if xuid == "" || xuid == ownerXUID {
			continue
		}
		active[xuid] = struct{}{}
	}

	changed := false
	for xuid := range nonces {
		if _, ok := active[xuid]; !ok {
			delete(nonces, xuid)
			changed = true
		}
	}
	for xuid := range active {
		if _, ok := nonces[xuid]; ok {
			continue
		}
		nonce, err := generate()
		if err != nil {
			return false, err
		}
		nonces[xuid] = nonce
		changed = true
	}
	return changed, nil
}

func generateSessionNonce() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// statusWithNonces shadows room.Status fields whose wire shape diverges from
// MCXboxBroadcast by adding isHardcore and the per-member nonce map.
type statusWithNonces struct {
	room.Status
	IsHardcore           bool              `json:"isHardcore"`
	SupportedConnections []p2p.Connection  `json:"SupportedConnections"`
	Nonces               map[string]string `json:"nonces"`
}

func marshalStatusWithNonces(status room.Status, nonces map[string]string) ([]byte, error) {
	connections := status.SupportedConnections
	status.SupportedConnections = nil
	return json.Marshal(statusWithNonces{
		Status:               status,
		SupportedConnections: connections,
		Nonces:               copyStringMap(nonces),
	})
}

func copyStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

func sessionMemberCount(session *mpsd.Session) int {
	count := 0
	for range session.Members() {
		count++
	}
	return count
}

// debugRoomStatus logs the room status at debug level using the given logger.
func debugRoomStatus(log *slog.Logger, msg string, status room.Status) {
	if !log.Enabled(context.Background(), slog.LevelDebug) {
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
