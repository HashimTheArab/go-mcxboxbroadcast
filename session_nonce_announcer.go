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

	"github.com/df-mc/go-xsapi/v2/mpsd"
	"github.com/google/uuid"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

// sessionNonceAnnouncer publishes the session with a per-member nonce map. The
// embedded XBLAnnouncer's mutex guards only Session and SessionReference, so
// readers never wait on MPSD; busy serializes the network writes instead.
type sessionNonceAnnouncer struct {
	*room.XBLAnnouncer

	ownerXUID string
	log       *slog.Logger

	busy chan struct{}

	// Guarded by busy.
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
		busy:         make(chan struct{}, 1),
		nonces:       make(map[string]string),
	}
}

// acquire waits for busy, giving up when ctx ends so callers keep their deadlines.
func (a *sessionNonceAnnouncer) acquire(ctx context.Context) error {
	select {
	case a.busy <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for in-flight session write: %w", ctx.Err())
	}
}

func (a *sessionNonceAnnouncer) release() { <-a.busy }

// session returns the published session under the XBLAnnouncer mutex.
func (a *sessionNonceAnnouncer) session() *mpsd.Session {
	a.Lock()
	defer a.Unlock()
	return a.Session
}

func (a *sessionNonceAnnouncer) setSession(session *mpsd.Session) {
	a.Lock()
	a.Session = session
	a.Unlock()
}

func (a *sessionNonceAnnouncer) Announce(ctx context.Context, status room.Status) error {
	if err := a.acquire(ctx); err != nil {
		return err
	}
	defer a.release()

	session := a.session()
	read, join := a.publishRestrictions(status)
	if session != nil && a.readRestriction == "" && a.joinRestriction == "" {
		if properties := session.Properties(); properties.System != nil {
			a.readRestriction = properties.System.ReadRestriction
			a.joinRestriction = properties.System.JoinRestriction
		}
	}

	// Reconcile nonces with the current members so a nonce write that failed
	// after a change notice is retried by the next announcement.
	nonces := copyStringMap(a.nonces)
	if session != nil {
		if _, err := syncSessionNonces(nonces, sessionMemberXUIDs(session), a.ownerXUID, generateSessionNonce); err != nil {
			return err
		}
	}
	custom, err := marshalStatusWithNonces(status, nonces)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	a.lastStatus = status
	if session != nil && bytes.Equal(custom, a.custom) && read == a.readRestriction && join == a.joinRestriction {
		a.handleSession(session)
		return nil
	}

	restrictionsChanged := read != a.readRestriction || join != a.joinRestriction
	if session != nil && restrictionsChanged {
		if a.Client == nil {
			return errors.New("room: XBLAnnouncer.Client is nil and MPSD restrictions changed")
		}
		if err := session.CloseContext(ctx); err != nil {
			return fmt.Errorf("close stale session: %w", err)
		}
		a.resetForRepublish(false)
		session = nil
		nonces = map[string]string{}
		if custom, err = marshalStatusWithNonces(status, nonces); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	}
	config, read, join := a.publishConfig(status, custom)

	if session == nil {
		if a.Client == nil {
			return errors.New("room: XBLAnnouncer.Client is nil")
		}
		a.Lock()
		if a.SessionReference.ServiceConfigID == uuid.Nil {
			a.SessionReference.ServiceConfigID = uuid.MustParse("4fc10100-5f7a-4470-899b-280835760c07")
		}
		if a.SessionReference.TemplateName == "" {
			a.SessionReference.TemplateName = "MinecraftLobby"
		}
		if a.SessionReference.Name == "" {
			a.SessionReference.Name = strings.ToUpper(uuid.NewString())
		}
		ref := a.SessionReference
		a.Unlock()
		published, err := a.Client.Publish(ctx, ref, config)
		if err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		session = published
		a.setSession(session)
	} else if err := session.SetCustomProperties(ctx, custom); err != nil {
		return fmt.Errorf("set custom properties: %w", err)
	}

	a.custom = custom
	a.nonces = nonces
	a.readRestriction = read
	a.joinRestriction = join
	a.handleSession(session)
	a.debug("published mpsd session nonces", "nonce_count", len(nonces))
	return nil
}

// Close closes the published session once any in-flight write finishes, so a
// session being published cannot be left open behind the close.
func (a *sessionNonceAnnouncer) Close() error {
	a.busy <- struct{}{}
	defer a.release()
	if session := a.session(); session != nil {
		return session.Close()
	}
	return nil
}

// discardSession makes the next Announce publish a new session under a fresh
// name when session is still the published one. The caller closes session.
func (a *sessionNonceAnnouncer) discardSession(ctx context.Context, session *mpsd.Session) (bool, error) {
	if err := a.acquire(ctx); err != nil {
		return false, err
	}
	defer a.release()
	if session == nil || a.session() != session {
		return false, nil
	}
	a.resetForRepublish(true)
	return true, nil
}

// resetForRepublish clears the published session state. A new name is needed
// when the old session may still exist, since publishing refuses to overwrite it.
// The caller must hold busy.
func (a *sessionNonceAnnouncer) resetForRepublish(rename bool) {
	a.Lock()
	a.Session = nil
	if rename {
		a.SessionReference.Name = strings.ToUpper(uuid.NewString())
	}
	a.Unlock()
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

// handleSession registers the nonce handler once per session. The caller must hold busy.
func (a *sessionNonceAnnouncer) handleSession(session *mpsd.Session) {
	if session == nil || session == a.handledSession {
		return
	}
	a.handledSession = session
	session.Handle(sessionNonceHandler{announcer: a})
}

func (a *sessionNonceAnnouncer) updateNoncesFromSession(ctx context.Context, session *mpsd.Session) error {
	return a.updateNonces(ctx, session, sessionMemberXUIDs(session), func(ctx context.Context, custom json.RawMessage) error {
		return session.SetCustomProperties(ctx, custom)
	})
}

// updateNonces publishes nonces for activeXUIDs, committing them only after
// the write succeeds so a failed write is retried by the next change or announcement.
func (a *sessionNonceAnnouncer) updateNonces(ctx context.Context, session *mpsd.Session, activeXUIDs []string, setCustomProperties func(context.Context, json.RawMessage) error) error {
	if err := a.acquire(ctx); err != nil {
		return err
	}
	defer a.release()
	if session != a.session() {
		return nil
	}
	nonces := copyStringMap(a.nonces)
	changed, err := syncSessionNonces(nonces, activeXUIDs, a.ownerXUID, generateSessionNonce)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	custom, err := marshalStatusWithNonces(a.lastStatus, nonces)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if err := setCustomProperties(ctx, custom); err != nil {
		return fmt.Errorf("set custom properties: %w", err)
	}
	a.custom = custom
	a.nonces = nonces
	a.debug(
		"updated mpsd session nonces",
		"nonce_count", len(nonces),
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
