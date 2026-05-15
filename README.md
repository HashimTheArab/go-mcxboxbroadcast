# broadcaster-go

`broadcaster-go` publishes a Minecraft: Bedrock Edition server as an Xbox Live
friend-list world and transfers clients that join the published NetherNet
session to the configured Bedrock server.

The library is modelled after MCXboxBroadcast while using Go-first building
blocks:

- `github.com/df-mc/go-xsapi` for Xbox Live MPSD/RTA session publishing.
- `github.com/df-mc/go-nethernet` for NetherNet/WebRTC listener support.
- the confirmed-stable `hashimthearab/gophertunnel` `lunar` branch for
  NetherNet, signaling, room announcements, and auth helpers.
- `FriendClient` for the Xbox follower/social endpoints used by friend sync.

## CLI

```sh
go run ./cmd/broadcaster -host play.example.net -port 19132 -name "Example" -world "Example World"
```

The first run starts Microsoft device-code authentication and stores the Live
token in the cache path printed by `-cache`. The tool refreshes the Xbox session
every 30 seconds and queries the target Bedrock server by RakNet ping unless
`-query=false` is set.

## Library

```go
live := auth.RefreshTokenSourceWriter(cachedLiveToken, os.Stdout)

b, err := broadcaster.New(broadcaster.Config{
    TokenSource:     broadcaster.NewXBLTokenSource(context.Background(), live),
    LiveTokenSource: live,
    Server: broadcaster.ServerInfo{
        Host: "play.example.net",
        Port: 19132,
    },
    Status: broadcaster.Status{
        HostName:    "Example",
        WorldName:   "Example World",
        Players:     1,
        MaxPlayers:  20,
        QueryTarget: true,
    },
})
if err != nil {
    return err
}
if err := b.Start(ctx); err != nil {
    return err
}
defer b.Close()
```

Contexts are accepted for start, update, signaling setup, announcement, and
shutdown-sensitive operations.
