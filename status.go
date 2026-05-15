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
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func (b *Broadcaster) status(ctx context.Context) (room.Status, error) {
	if b.conf.StatusProvider != nil {
		return normalizeStatus(b.conf.StatusProvider.RoomStatus()), nil
	}
	st := b.conf.Status
	if st.QueryTarget {
		if queried, err := queryStatusWithFallback(ctx, QueryOptions{
			Address:            b.conf.Server.Address(),
			Timeout:            st.QueryTimeout,
			WebFallbackEnabled: st.WebQueryFallback,
			Client:             st.WebQueryClient,
		}); err == nil {
			st.WorldName = queried.ServerName
			st.HostName = queried.ServerSubName
			st.Players = queried.PlayerCount
			st.MaxPlayers = queried.MaxPlayers
		} else if !st.QueryFallback {
			return room.Status{}, err
		}
	}

	return normalizeStatus(room.Status{
		HostName:                stripColour(defaultString(st.HostName, "MCXboxBroadcast")),
		WorldName:               stripColour(defaultString(st.WorldName, defaultString(st.HostName, "MCXboxBroadcast"))),
		WorldType:               defaultString(st.WorldType, room.WorldTypeCreative),
		MemberCount:             max(st.Players, 1),
		MaxMemberCount:          max(st.MaxPlayers, max(st.Players, 1)+1),
		BroadcastSetting:        defaultInt32(st.Broadcast, room.BroadcastSettingFriendsOfFriends),
		Joinability:             defaultString(st.Joinability, room.JoinabilityJoinableByFriends),
		Protocol:                protocol.CurrentProtocol,
		Version:                 protocol.CurrentVersion,
		TitleID:                 TitleID,
		LanGame:                 false,
		OnlineCrossPlatformGame: true,
		CrossPlayDisabled:       false,
		LevelID:                 levelID(st.LevelID),
	}), nil
}

func normalizeStatus(status room.Status) room.Status {
	defaults := room.DefaultStatus()
	if status.HostName == "" {
		status.HostName = defaults.HostName
	}
	status.HostName = stripColour(status.HostName)
	if status.WorldName == "" {
		status.WorldName = status.HostName
	}
	status.WorldName = stripColour(status.WorldName)
	if status.WorldType == "" {
		status.WorldType = room.WorldTypeCreative
	}
	if status.MemberCount <= 0 {
		status.MemberCount = 1
	}
	if status.MaxMemberCount <= status.MemberCount {
		status.MaxMemberCount = status.MemberCount + 1
	}
	if status.BroadcastSetting == 0 {
		status.BroadcastSetting = room.BroadcastSettingFriendsOfFriends
	}
	if status.Joinability == "" {
		status.Joinability = room.JoinabilityJoinableByFriends
	}
	if status.Protocol == 0 {
		status.Protocol = protocol.CurrentProtocol
	}
	if status.Version == "" {
		status.Version = protocol.CurrentVersion
	}
	if status.TitleID == 0 {
		status.TitleID = TitleID
	}
	status.OnlineCrossPlatformGame = true
	if status.LevelID == "" {
		status.LevelID = defaults.LevelID
	}
	return status
}

type normalizedStatusProvider struct {
	Provider room.StatusProvider
}

func (p normalizedStatusProvider) RoomStatus() room.Status {
	if p.Provider == nil {
		return normalizeStatus(room.Status{})
	}
	return normalizeStatus(p.Provider.RoomStatus())
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
		timeout = 5 * time.Second
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

func defaultInt32(v, d int32) int32 {
	if v == 0 {
		return d
	}
	return v
}
