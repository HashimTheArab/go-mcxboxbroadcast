# go-mcxboxbroadcast

`go-mcxboxbroadcast` publishes a Minecraft: Bedrock Edition server as an Xbox Live
friend-list world and transfers clients that join the published NetherNet
session to the configured Bedrock server.

## Acknowledgements

This project is a Go port inspired by the original
[MCXboxBroadcast](https://github.com/rtm516/MCXboxBroadcast) work and the
[GeyserMC](https://geysermc.org/) ecosystem. Credit goes to the GeyserMC
project and contributors for the Geyser Bedrock listener behavior and
configuration model that this implementation follows.

The library is modelled after
[MCXboxBroadcast](https://github.com/rtm516/MCXboxBroadcast) while using
Go-first building blocks:

- `github.com/df-mc/go-xsapi/v2` for Xbox Live MPSD/RTA session publishing.
- `github.com/df-mc/go-nethernet` for NetherNet/WebRTC listener support.
- `github.com/sandertv/gophertunnel`, using the
  `HashimTheArab/gophertunnel` fork, for NetherNet, signaling, room
  announcements, and `minecraft/p2p`-compatible session metadata.
- `github.com/sandertv/go-raknet`, using the `HashimTheArab/go-raknet` fork,
  for RakNet ping compatibility.

## CLI

```sh
go run ./cmd/broadcaster -config config.yml
```

If `config.yml` does not exist, the command writes a default one and starts from
those values. The first run starts Microsoft device-code authentication and
stores the Live token at `accounts.primaryCachePath`.

Configuration keys use the exact camelCase names shown in
[`config.example.yml`](config.example.yml). YAML and TOML are supported; legacy
kebab-case keys, root-level session settings, friend-expiry aliases, and
`slack-webhook` are not translated.

Use `-debug` or set `debugMode: true` in the config to show detailed runtime
events such as session creation, presence heartbeats, friend sync scans, pending
friend-request accepts, friends being added/removed, and the final add/remove
counts for each sync pass.

The config exposes the same operator-facing areas as MCXboxBroadcast:

- session target, update interval, query options, broadcast setting,
  world type, and displayed MOTD data
- gallery showcase image upload through `gallery.imagePath`
- friend sync automation and expiry settings, including last-seen history path
  (stored as JSON at `friendSync.expiry.historyPath`, not Java's SQLite
  database — operators migrating from MCXboxBroadcast start with a fresh
  expiry history)
- Slack/Discord-compatible webhook notifications
- primary and sub-account token cache paths
- optional HTTP proxy URL through `http.proxy`
- selectable NetherNet signaling through `session.signalingMode`: `websocket`
  (default) or `jsonrpc` (only when no sub-accounts are enabled).
- relay mode through `relay.enabled`, which keeps players inside the NetherNet
  session instead of transferring them (see below).
- an optional health endpoint through `health.listen` (see below).

### Session recovery

The broadcaster automatically recovers lost signaling connections, repeated
session-update failures, Xbox sessions that are closed or deleted, and missing
or closed Xbox activity advertisements. When only the Xbox session is affected,
it publishes a new session and keeps the signaling connection and listener, so
players who are joining are not dropped. Activity checks allow time for Xbox to
show new sessions and use a cooldown to avoid repeated restarts. A missing
sub-account advertisement only recreates that sub-account's session. Recovery
failures appear in the logs and are sent to the configured webhook.

These checks use the publishing account's credentials. They detect a missing
directory advertisement, but do not verify friendship permissions or prove that
another player can join. Keep an independent friend-account monitor for that.

If recovery keeps failing, the command closes its resources and exits with an
error. Run it under Kubernetes or another process supervisor with automatic
restart enabled so the next process recreates authentication and client state.

### Health checks

Set `health.listen` (for example `":8080"`) to serve probe endpoints:

- `/healthz` fails when no Xbox session update has succeeded for five update
  intervals (at least five minutes). Use it as a liveness probe.
- `/readyz` also fails while the broadcaster is starting or recovering.

Before the first session is published, `/healthz` succeeds so a sign-in that
waits for a device code is not restarted. Outbound HTTP requests time out
after 30 seconds, so a stuck Xbox call fails instead of hanging.

### Relay mode

By default a joining client receives a `Transfer` to `sessionInfo.ip:port` and
leaves the Xbox session, so only the bot's own friends ever see the world. With
`relay.enabled: true` the broadcaster instead logs the player into the backend
itself and relays every packet batch in both directions. The player stays a
member of the session for as long as they play, which lets their own friends
discover and join the world too. The backend owns the whole login sequence,
including resource packs, so the client sees exactly what a direct join would.

The backend receives the relay's address and a self-signed login chain that
still carries the XUID the broadcaster verified, so it must trust the relay:
Geyser with `advanced.bedrock.validate-bedrock-login: false`, BDS with
`online-mode=false`, or a gophertunnel listener with `AuthenticationDisabled`.
Bind such a backend to loopback or a private network; the broadcaster is its
authentication boundary. Public servers that verify login chains cannot be
relayed to, and a `Transfer` sent by the backend still moves the client out of
the session.

Library users can route each player individually with `RelayConfig.ResolveTarget`
and customize the backend dial with `RelayConfig.Dialer`. Xbox Live's session
member limit bounds how many players one session can relay at a time.

### Signaling modes

`session.signalingMode` defaults to `websocket`. Use `jsonrpc` to connect through
Player Messaging instead. JSON-RPC does not support sub-accounts; startup fails
if both are enabled.

## Docker

The standalone container is published at
`ghcr.io/hashimthearab/go-mcxboxbroadcast:latest`.

```sh
docker run --rm -it -v /path/to/config:/opt/app/config ghcr.io/hashimthearab/go-mcxboxbroadcast:latest
```

Interactive terminals use colored, human-readable logs. Redirected output and
container log streams use plain structured text. Set `NO_COLOR=1` to disable
color or `FORCE_COLOR=1` to enable it for consoles that do not expose a TTY.

The mounted config directory is where the app reads or creates `config.yml` and
stores token cache, player history, and gallery assets. With the default
configuration, putting `screenshot.jpg` in that directory makes it the showcased
image.

## Pterodactyl

Import `deployments/pterodactyl/egg-go-mcxboxbroadcast.json` into a Pterodactyl
nest to run the broadcaster from the panel. The egg uses the Pterodactyl-specific
image published at `ghcr.io/hashimthearab/go-mcxboxbroadcast:pterodactyl`,
which runs as the required `container` user from `/home/container`.

The egg creates `config.yml` on install and exposes the target Bedrock
host/port, displayed server names, query behavior, notifications, and gallery
settings as panel variables. Account tokens, friend history, and other runtime
state are stored under `/home/container/cache`. On first start, complete the
Microsoft device-code sign-in shown in the console; if notifications are
enabled, the sign-in prompt is also sent to the configured webhook.

## Library

Leave `Config.Signaling` unset to allow automatic primary-session recovery.
Use `SignalingFactory` if you need custom signaling that can be recreated.
Supplying a fixed `Config.Signaling` connection disables signaling
recreation. Lost Xbox sessions and missing activity advertisements are still
repaired by publishing a new session over that connection.

`Broadcaster.Wait()` returns any terminal recovery error. Call `Close()` afterward
to release resources. A normal shutdown returns no recovery error.
`Broadcaster.HealthHandler()` serves the same `/healthz` and `/readyz` probes as
the CLI. Close the token source from `NewMinecraftTokenSource` when you are done
with it.

```go
live := auth.RefreshTokenSourceWriter(cachedLiveToken, os.Stdout)
xblSource := broadcaster.NewXBLTokenSource(ctx, live)
xblClient, err := broadcaster.NewXSAPIClient(ctx, xblSource, http.DefaultClient, nil)
if err != nil {
    return err
}
minecraftTokens, err := broadcaster.NewMinecraftTokenSource(ctx, xblClient, http.DefaultClient)
if err != nil {
    return err
}

b, err := broadcaster.New(broadcaster.Config{
    XBLClient:           xblClient,
    XBLTokenSource:      xblSource,
    XUID:                 xblClient.UserInfo().XUID,
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
runErr := b.Wait()
closeErr := b.Close()
return errors.Join(runErr, closeErr, minecraftTokens.Close())
```

Contexts are accepted for start, update, signaling setup, announcement, and
shutdown-sensitive operations.

## Development

See [AGENTS.md](AGENTS.md) for dependency guidance, implementation constraints,
and validation commands.
