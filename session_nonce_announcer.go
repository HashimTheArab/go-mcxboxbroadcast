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
	"strconv"
	"strings"
	"time"

	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

type sessionNonceAnnouncer struct {
	*room.XBLAnnouncer

	ownerXUID string
	log       *slog.Logger

	custom          []byte
	readRestriction string
	joinRestriction string
	nonces          map[string]string
	lastStatus      room.Status
	handledSession  *mpsd.Session
}

func newSessionNonceAnnouncer(announcer *room.XBLAnnouncer, ownerXUID string, log *slog.Logger) *sessionNonceAnnouncer {
	return &sessionNonceAnnouncer{
		XBLAnnouncer: announcer,
		ownerXUID:    ownerXUID,
		log:          log,
		nonces:       make(map[string]string),
	}
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
			return errors.New("room: XBLAnnouncer.Client is nil and MPSD restrictions changed")
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
			return errors.New("room: XBLAnnouncer.Client is nil")
		}
		if a.SessionReference.ServiceConfigID == uuid.Nil {
			a.SessionReference.ServiceConfigID = uuid.MustParse("4fc10100-5f7a-4470-899b-280835760c07")
		}
		if a.SessionReference.TemplateName == "" {
			a.SessionReference.TemplateName = "MinecraftLobby"
		}
		if a.SessionReference.Name == "" {
			a.SessionReference.Name = strings.ToUpper(uuid.NewString())
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
	a.debug("published mpsd session nonces", "nonce_count", len(a.nonces))
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
	read, join = sessionRestrictions(status.BroadcastSetting)
	if a.PublishConfig.ReadRestriction != "" {
		read = a.PublishConfig.ReadRestriction
	}
	if a.PublishConfig.JoinRestriction != "" {
		join = a.PublishConfig.JoinRestriction
	}
	return read, join
}

func sessionRestrictions(setting p2p.BroadcastSetting) (read, join string) {
	switch setting {
	case p2p.BroadcastSettingFriendsOfFriends, p2p.BroadcastSettingFriendsOnly:
		return mpsd.SessionRestrictionFollowed, mpsd.SessionRestrictionFollowed
	case p2p.BroadcastSettingInviteOnly:
		return mpsd.SessionRestrictionLocal, mpsd.SessionRestrictionFollowed
	default:
		return mpsd.SessionRestrictionFollowed, mpsd.SessionRestrictionFollowed
	}
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
	a.debug(
		"updated mpsd session nonces",
		"nonce_count", len(a.nonces),
		"active_member_count", len(activeXUIDs),
	)
	return nil
}

func (a *sessionNonceAnnouncer) debug(msg string, args ...any) {
	if a.log != nil {
		a.log.Debug(msg, args...)
	}
}

type sessionNonceHandler struct {
	announcer *sessionNonceAnnouncer
}

func (h sessionNonceHandler) HandleSessionChange(session *mpsd.Session) {
	ctx, cancel := context.WithTimeout(session.Context(), 15*time.Second)
	defer cancel()
	if err := h.announcer.updateNoncesFromSession(ctx, session); err != nil {
		if h.announcer.log != nil {
			h.announcer.log.Error("update mpsd session nonces", "err", err)
		}
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
	if nonces == nil {
		return false, errors.New("nonces map is nil")
	}
	if generate == nil {
		generate = generateSessionNonce
	}
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
// MCXboxBroadcast: isHardcore is added (Java always sends false) and
// SupportedConnections re-encodes NetherNetId as a JSON number.
type statusWithNonces struct {
	room.Status
	IsHardcore           bool                `json:"isHardcore"`
	SupportedConnections []sessionConnection `json:"SupportedConnections"`
	Nonces               map[string]string   `json:"nonces"`
}

// sessionConnection mirrors room.Connection with a numeric NetherNetId,
// matching the Java session document.
type sessionConnection struct {
	ConnectionType int             `json:"ConnectionType"`
	HostIPAddress  string          `json:"HostIpAddress"`
	HostPort       uint16          `json:"HostPort"`
	NetherNetID    json.RawMessage `json:"NetherNetId"`
	RakNetGUID     string          `json:"RakNetGUID,omitempty"`
	PmsgID         uuid.UUID       `json:"PmsgId,omitempty"`
}

func sessionConnections(connections []room.Connection) []sessionConnection {
	out := make([]sessionConnection, 0, len(connections))
	for _, connection := range connections {
		out = append(out, sessionConnection{
			ConnectionType: connection.ConnectionType,
			HostIPAddress:  connection.HostIPAddress,
			HostPort:       connection.HostPort,
			NetherNetID:    netherNetIDValue(connection.NetherNetID),
			RakNetGUID:     connection.RakNetGUID,
			PmsgID:         connection.PmsgID,
		})
	}
	return out
}

// netherNetIDValue encodes a NetherNet ID as a JSON number when it is one,
// falling back to a string for non-numeric values.
func netherNetIDValue(id p2p.NetherNetID) json.RawMessage {
	if _, err := strconv.ParseUint(string(id), 10, 64); err == nil {
		return json.RawMessage(id)
	}
	quoted, err := json.Marshal(string(id))
	if err != nil {
		return json.RawMessage(`"0"`)
	}
	return quoted
}

func marshalStatusWithNonces(status room.Status, nonces map[string]string) ([]byte, error) {
	connections := sessionConnections(status.SupportedConnections)
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
