package broadcaster

import (
	"context"
	"encoding/base64"
	"strings"
	"time"

	"github.com/sandertv/go-raknet"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/room"
)

func (b *Broadcaster) status(ctx context.Context) (room.Status, error) {
	st := b.conf.Status
	if st.QueryTarget {
		if queried, err := queryStatus(ctx, b.conf.Server.Address(), st.QueryTimeout); err == nil {
			st.WorldName = queried.ServerName
			st.HostName = queried.ServerSubName
			st.Players = queried.PlayerCount
			st.MaxPlayers = queried.MaxPlayers
		} else if !st.QueryFallback {
			return room.Status{}, err
		}
	}

	status := room.DefaultStatus()
	status.HostName = stripColour(defaultString(st.HostName, "MCXboxBroadcast"))
	status.WorldName = stripColour(defaultString(st.WorldName, status.HostName))
	status.WorldType = defaultString(st.WorldType, room.WorldTypeCreative)
	status.MemberCount = max(st.Players, 1)
	status.MaxMemberCount = max(st.MaxPlayers, status.MemberCount+1)
	status.BroadcastSetting = defaultInt32(st.Broadcast, room.BroadcastSettingFriendsOfFriends)
	status.Joinability = defaultString(st.Joinability, room.JoinabilityJoinableByFriends)
	status.Protocol = protocol.CurrentProtocol
	status.Version = protocol.CurrentVersion
	status.TitleID = TitleID
	status.LanGame = false
	status.OnlineCrossPlatformGame = true
	status.CrossPlayDisabled = false
	if st.LevelID != "" {
		status.LevelID = base64.StdEncoding.EncodeToString([]byte(st.LevelID))
	}
	return status, nil
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
