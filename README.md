# broadcaster-go

`broadcaster-go` publishes a Minecraft: Bedrock Edition server as an Xbox Live
friend-list world and transfers clients that join the published NetherNet
session to the configured Bedrock server.

The library is modelled after
[MCXboxBroadcast](https://github.com/rtm516/MCXboxBroadcast) while using
Go-first building blocks:

- `github.com/df-mc/go-xsapi` for Xbox Live MPSD/RTA session publishing.
- `github.com/df-mc/go-nethernet` for NetherNet/WebRTC listener support.
- `hashimthearab/gophertunnel` `lunar` branch for NetherNet, signaling, room
  announcements, and auth helpers. This should be updated to the official
  `sandertv/gophertunnel` once it supports Xbox friend-list NetherNet
  signaling.

## Acknowledgements

This project is a Go port inspired by the original
[MCXboxBroadcast](https://github.com/rtm516/MCXboxBroadcast) work and the
[GeyserMC](https://geysermc.org/) ecosystem. Credit goes to the GeyserMC
project and contributors for the Geyser Bedrock listener behavior and
configuration model that this implementation follows.

## CLI

```sh
go run ./cmd/broadcaster -config config.yml
```

If `config.yml` does not exist, the command writes a default one and starts from
those values. The first run starts Microsoft device-code authentication and
stores the Live token at `accounts.primaryCachePath`.

Use `-debug` or set `debugMode: true` in the config to show detailed runtime
events such as session creation, presence heartbeats, friend sync scans, pending
friend-request accepts, friends being added/removed, and the final add/remove
counts for each sync pass.

The config exposes the same operator-facing areas as MCXboxBroadcast:

- session target, remote address/port auto behavior, update interval, query
  options, broadcast setting, joinability, world type, and displayed MOTD data
- gallery showcase image upload through `gallery.imagePath`
- friend sync automation and expiry settings, including last-seen history path
- Slack/Discord-compatible webhook notifications
- primary and sub-account token cache paths
- optional HTTP proxy URL through `http.proxy`
- NetherNet signaling mode through `session.signalingMode`; `jsonrpc` is the
  default and matches MCXboxBroadcast's `ConnectionType=7`/`PmsgId` session
  metadata, while `websocket` keeps the older signaling service path.

## Docker

The standalone container is published at
`ghcr.io/hashimthearab/go-mcxboxbroadcast:latest`.

```sh
docker run --rm -it -v /path/to/config:/opt/app/config ghcr.io/hashimthearab/go-mcxboxbroadcast:latest
```

The mounted config directory is where the app reads or creates `config.yml` and
stores token cache, player history, and gallery assets. With the default
configuration, putting `screenshot.jpg` in that directory makes it the showcased
image.

## Library

```go
live := auth.RefreshTokenSourceWriter(cachedLiveToken, os.Stdout)
minecraftTokens, err := broadcaster.NewMinecraftTokenSource(ctx, live, http.DefaultClient)
if err != nil {
    return err
}

b, err := broadcaster.New(broadcaster.Config{
    TokenSource:          broadcaster.NewXBLTokenSource(context.Background(), live),
    LiveTokenSource:      live,
    MinecraftTokenSource: minecraftTokens,
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
    Gallery: &broadcaster.GalleryConfig{
        Enabled:   true,
        ImagePath: "screenshot.jpg",
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
