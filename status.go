package broadcaster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sandertv/go-raknet"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/p2p"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

var defaultRoomStatus = room.DefaultStatus()

func (b *Broadcaster) status(ctx context.Context) (room.Status, error) {
	ownerID, err := b.primaryXUIDForStatus(ctx)
	if err != nil {
		return room.Status{}, err
	}
	if b.conf.StatusProvider != nil {
		return normalizeStatusWithOwner(b.conf.StatusProvider.RoomStatus(), ownerID), nil
	}
	st := b.conf.Status
	if st.QueryTarget {
		b.debug("querying target server status", "target", b.conf.Server.Address(), "web_fallback", st.WebQueryFallback, "query_fallback", st.QueryFallback)
		if queried, err := queryStatusWithFallback(ctx, QueryOptions{
			Address:            b.conf.Server.Address(),
			Timeout:            st.QueryTimeout,
			WebFallbackEnabled: st.WebQueryFallback,
			Client:             st.WebQueryClient,
		}); err == nil {
			b.debug("queried target server status",
				"server_name", queried.ServerName,
				"server_sub_name", queried.ServerSubName,
				"players", queried.PlayerCount,
				"max_players", queried.MaxPlayers,
			)
			b.lastQuery = &queried
			applyQueriedStatus(&st, queried)
		} else if !st.QueryFallback && b.lastQuery != nil {
			// Keep the last successful query result instead of failing the
			// whole update; configFallback: true resets to configured values.
			b.debug("target server status query failed; keeping last successful query result", "err", err)
			applyQueriedStatus(&st, *b.lastQuery)
		} else {
			b.debug("target server status query failed; using configured status fallback", "err", err)
		}
	}

	return normalizeStatus(room.Status{
		HostName:                stripColour(defaultString(st.HostName, b.hostNameFallback())),
		WorldName:               stripColour(defaultString(st.WorldName, defaultString(st.HostName, b.hostNameFallback()))),
		OwnerID:                 ownerID,
		WorldType:               defaultString(st.WorldType, WorldTypeSurvival),
		MemberCount:             max(st.Players, 0),
		MaxMemberCount:          max(st.MaxPlayers, max(st.Players, 0)+1),
		BroadcastSetting:        defaultBroadcastSetting(p2p.BroadcastSetting(st.Broadcast), p2p.BroadcastSettingFriendsOfFriends),
		Joinability:             defaultString(st.Joinability, p2p.JoinabilityFriends),
		Protocol:                protocol.CurrentProtocol,
		Version:                 protocol.CurrentVersion,
		TransportLayer:          p2p.TransportLayerNetherNet,
		LanGame:                 false,
		OnlineCrossPlatformGame: true,
		CrossPlayDisabled:       false,
		LevelID:                 levelID(st.LevelID),
	}), nil
}

// applyQueriedStatus overlays a queried server status onto the announced one.
func applyQueriedStatus(st *Status, queried minecraft.ServerStatus) {
	st.WorldName = queried.ServerName
	st.HostName = queried.ServerSubName
	st.Players = queried.PlayerCount
	st.MaxPlayers = queried.MaxPlayers
}

// hostNameFallback returns the account gamertag for empty host names, matching
// MCXboxBroadcast, with a static fallback when no profile is available.
func (b *Broadcaster) hostNameFallback() string {
	client := b.conf.XBLClient
	if client == nil {
		client = b.xblClient
	}
	if client != nil {
		if gamertag := client.UserInfo().GamerTag; gamertag != "" {
			return gamertag
		}
	}
	return "MCXboxBroadcast"
}

func (b *Broadcaster) primaryXUIDForStatus(ctx context.Context) (string, error) {
	if xuid := b.primaryXUID(); xuid != "" {
		return xuid, nil
	}
	if b.conf.XBLTokenSource == nil {
		return "", nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	token, err := b.conf.XBLTokenSource.XSTSToken(ctx, xboxLiveRelyingParty)
	if err != nil {
		return "", fmt.Errorf("resolve xbox live xuid: %w", err)
	}
	if token == nil {
		return "", fmt.Errorf("resolve xbox live xuid: token source returned nil XSTS token")
	}
	xuid := token.UserInfo().XUID
	b.conf.XUID = xuid
	return xuid, nil
}

func normalizeStatusWithOwner(status room.Status, ownerID string) room.Status {
	status = normalizeStatus(status)
	if status.OwnerID == "" {
		status.OwnerID = ownerID
	}
	return status
}

func normalizeStatus(status room.Status) room.Status {
	defaults := defaultRoomStatus
	if status.HostName == "" {
		status.HostName = defaults.HostName
	}
	status.HostName = stripColour(status.HostName)
	if status.WorldName == "" {
		status.WorldName = status.HostName
	}
	status.WorldName = stripColour(status.WorldName)
	if status.WorldType == "" {
		status.WorldType = WorldTypeSurvival
	}
	if status.MemberCount < 0 {
		status.MemberCount = 0
	}
	if status.MaxMemberCount <= status.MemberCount {
		status.MaxMemberCount = status.MemberCount + 1
	}
	if status.BroadcastSetting == 0 {
		status.BroadcastSetting = p2p.BroadcastSettingFriendsOfFriends
	}
	if status.Joinability == "" {
		status.Joinability = p2p.JoinabilityFriends
	}
	if status.Protocol == 0 {
		status.Protocol = protocol.CurrentProtocol
	}
	if status.Version == "" {
		status.Version = protocol.CurrentVersion
	}
	// Minecraft friend-list sessions use TitleId=0 in MPSD custom properties.
	// The package TitleID constant is still used for Xbox invite handles.
	status.TitleID = 0
	status.TransportLayer = p2p.TransportLayerNetherNet
	status.OnlineCrossPlatformGame = true
	if status.LevelID == "" {
		// MCXboxBroadcast always sends the literal "level".
		status.LevelID = "level"
	}
	return status
}

type normalizedStatusProvider struct {
	Provider room.StatusProvider
	OwnerID  string
}

func (p normalizedStatusProvider) RoomStatus() room.Status {
	if p.Provider == nil {
		return normalizeStatusWithOwner(room.Status{}, p.OwnerID)
	}
	return normalizeStatusWithOwner(p.Provider.RoomStatus(), p.OwnerID)
}

type roomMinecraftStatusProvider struct {
	Provider room.StatusProvider
}

func (p roomMinecraftStatusProvider) ServerStatus(int, int) minecraft.ServerStatus {
	status := normalizeStatus(p.Provider.RoomStatus())
	return minecraft.ServerStatus{
		ServerName:    status.WorldName,
		ServerSubName: status.HostName,
		PlayerCount:   status.MemberCount,
		MaxPlayers:    status.MaxMemberCount,
	}
}

func levelID(id string) string {
	if id == "" {
		return ""
	}
	return base64.StdEncoding.EncodeToString([]byte(id))
}

type QueryOptions struct {
	Address            string
	Timeout            time.Duration
	WebFallbackEnabled bool
	Client             *http.Client
}

func queryStatusWithFallback(ctx context.Context, options QueryOptions) (minecraft.ServerStatus, error) {
	status, err := queryStatus(ctx, options.Address, options.Timeout)
	if err == nil || !options.WebFallbackEnabled {
		return status, err
	}
	return webQueryStatus(ctx, options)
}

func webQueryStatus(ctx context.Context, options QueryOptions) (minecraft.ServerStatus, error) {
	if options.Timeout == 0 {
		options.Timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, options.Timeout)
	defer cancel()
	u := "https://checker.geysermc.org/ping?hostname=" + url.QueryEscape(hostOnly(options.Address))
	if _, port, err := net.SplitHostPort(options.Address); err == nil {
		u += "&port=" + url.QueryEscape(port)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return minecraft.ServerStatus{}, err
	}
	resp, err := httpClient(options.Client).Do(req)
	if err != nil {
		return minecraft.ServerStatus{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return minecraft.ServerStatus{}, fmt.Errorf("%s %s: %s", req.Method, req.URL, resp.Status)
	}
	var data struct {
		Success bool `json:"success"`
		Ping    struct {
			Pong struct {
				MOTD               string `json:"motd"`
				SubMOTD            string `json:"subMotd"`
				PlayerCount        int    `json:"playerCount"`
				MaximumPlayerCount int    `json:"maximumPlayerCount"`
			} `json:"pong"`
		} `json:"ping"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return minecraft.ServerStatus{}, err
	}
	if !data.Success {
		return minecraft.ServerStatus{}, fmt.Errorf("web query failed")
	}
	return minecraft.ServerStatus{
		ServerName:    data.Ping.Pong.MOTD,
		ServerSubName: data.Ping.Pong.SubMOTD,
		PlayerCount:   data.Ping.Pong.PlayerCount,
		MaxPlayers:    data.Ping.Pong.MaximumPlayerCount,
	}, nil
}

func hostOnly(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err == nil {
		return host
	}
	return address
}

func httpClient(c *http.Client) *http.Client {
	if c != nil {
		return c
	}
	return http.DefaultClient
}

func queryStatus(ctx context.Context, address string, timeout time.Duration) (minecraft.ServerStatus, error) {
	if timeout == 0 {
		// MCXboxBroadcast pings with a 1500 ms timeout.
		timeout = 1500 * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ch := make(chan struct {
		status minecraft.ServerStatus
		err    error
	}, 1)
	go func() {
		data, err := raknet.Ping(address)
		if err != nil {
			ch <- struct {
				status minecraft.ServerStatus
				err    error
			}{err: err}
			return
		}
		ch <- struct {
			status minecraft.ServerStatus
			err    error
		}{status: minecraft.ParsePongData(data)}
	}()

	select {
	case out := <-ch:
		return out.status, out.err
	case <-ctx.Done():
		return minecraft.ServerStatus{}, ctx.Err()
	}
}

func stripColour(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	skip := false
	for _, r := range s {
		if skip {
			skip = false
			continue
		}
		if r == '§' {
			skip = true
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func defaultString(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func defaultBroadcastSetting(v, d p2p.BroadcastSetting) p2p.BroadcastSetting {
	if v == 0 {
		return d
	}
	return v
}
