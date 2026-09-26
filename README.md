# go-mcxboxbroadcast

`go-mcxboxbroadcast` publishes a Minecraft: Bedrock Edition server as an Xbox Live
friend-list world and transfers clients that join the published NetherNet
session to the configured Bedrock server.

The library is modelled after
[MCXboxBroadcast](https://github.com/rtm516/MCXboxBroadcast) while using
Go-first building blocks:

- `github.com/df-mc/go-xsapi/v2` for Xbox Live MPSD/RTA session publishing,
  replaced in `go.mod` with the `HashimTheArab/go-xsapi` fork.
- `github.com/df-mc/go-nethernet` for NetherNet/WebRTC listener support. The
  upstream module is used directly because it now contains the networking
  changes that previously required Lunar's fork.
- `hashimthearab/gophertunnel` Lunar P2P branch for NetherNet, signaling,
  room announcements, and `minecraft/p2p`-compatible session metadata. This
  should be updated to the official `sandertv/gophertunnel` once it supports
  Xbox friend-list NetherNet signaling.
- `sandertv/go-raknet`, replaced in `go.mod` with the `hashimthearab/go-raknet`
  fork for RakNet ping compatibility.

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

If `config.yml` does not exist, the command writes a default one and exits so
you can set `session.sessionInfo.ip` and `port`; it refuses to start while the
target is still the example host. The first run starts Microsoft device-code
authentication and stores the Live token at `accounts.primaryCachePath`.

Configuration keys use the exact camelCase names shown in
[`config.example.yml`](config.example.yml). YAML and TOML are supported. Unknown
keys, including legacy kebab-case keys and `slack-webhook`, stop startup with an
error naming the key. Older `configVersion`s are migrated and rewritten on load.
Sessions always advertise the Minecraft version this build accepts.

Use `-debug` or set `debugMode: true` in the config to show detailed runtime
events such as session creation, presence heartbeats, friend sync scans, pending
friend-request accepts, friends being added/removed, and the final add/remove
counts for each sync pass.

The config exposes the same operator-facing areas as MCXboxBroadcast:

- session target, update interval, query options, broadcast setting,
  world type, and displayed MOTD data (joinability is always
  `joinable_by_friends`, matching MCXboxBroadcast)
- gallery showcase image upload through `gallery.imagePath`
- friend sync automation and friend list cleanup (see below)
- Slack/Discord-compatible webhook notifications
- primary and sub-account token cache paths
- optional HTTP proxy URL through `http.proxy`
- selectable NetherNet signaling through `session.signalingMode`: `websocket`
  (default) or `jsonrpc` (only when no sub-accounts are enabled).
- relay mode through `relay.enabled`, which keeps players inside the NetherNet
  session instead of transferring them (see below).

### Friend list cleanup

Xbox allows an account at most 1000 friends. Once the list is full, new players
can't add the bot. `friendSync.cleanup` keeps room for them:

```yaml
friendSync:
  cleanup:
    inactiveDays: 15   # remove friends not seen for this many days; 0 = off
    maxFriends: 950    # keep friends plus pending requests at or below this; 0 = off
    interval: 1800     # seconds between inactive-friend checks
    historyPath: cache/player_history.json
```

`maxFriends` is checked on every friend sync. When friends plus waiting friend
requests would go over it, the bot removes the friends it has seen least
recently to make exactly that much room, then accepts the waiting requests. It
never removes friends below `maxFriends`. A lower value leaves space for
requests that arrive between syncs. Removal ends the friendship
in both directions, so a removed player is not followed back. The bot's own
primary and sub-accounts are never removed.

"Seen" means the player's last join, or when the bot first tracked them as a
friend. Each account keeps its own history in the JSON file at `historyPath`.
This is not Java's SQLite database, so operators migrating from MCXboxBroadcast
start with a fresh history.

Configs from before `configVersion: 5` are migrated from `friendSync.expiry`.
They keep their inactivity setting, and `maxFriends` stays `0` until you set it.

### Session recovery

Signaling loss and repeated primary-session update failures share one recovery
loop. Normal updates pause while it rebuilds the session. Recovery makes up to
six attempts, waiting 5, 10, 20, 40, and 80 seconds between failures. It reports
the first recovery failure to the webhook and sends a recovery notice if a later
attempt succeeds; retry details remain in the logs.

If all six attempts fail, the command closes its resources and exits with an
error. Run it under Kubernetes or another process supervisor with automatic
restart enabled so the next process recreates authentication and client state.
Library callers receive the terminal error from `Broadcaster.Wait()` and must
call `Close()` to release resources. A normal shutdown returns no recovery error.

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

Both modes exchange the same WebRTC offers, answers, and ICE candidates.
`websocket` connects directly to the signaling service and advertises type `3`,
whose numeric network ID vanilla stores in `RakNetGUID`. `jsonrpc` wraps the
same messages in Player Messaging envelopes and advertises type `7` with
`PmsgId` and `NetherNetId`. JSON-RPC cannot be combined with enabled
sub-accounts because each independently owned session needs its own Player
Messaging identity; startup fails instead of publishing a misleading shared
identity.

## Docker

The standalone container is published at
`ghcr.io/hashimthearab/go-mcxboxbroadcast:latest`.

```sh
docker volume create mcxboxbroadcast
docker run --rm -it -v mcxboxbroadcast:/opt/app/config ghcr.io/hashimthearab/go-mcxboxbroadcast:latest
```

The container runs as UID 100, GID 101. A named volume inherits that ownership;
a bind-mounted host directory must be writable by it, or run the container as
the directory's owner:

```sh
docker run --rm -it --user "$(id -u):$(id -g)" -v /path/to/config:/opt/app/config ghcr.io/hashimthearab/go-mcxboxbroadcast:latest
```

Images are tagged `latest` and with the commit SHA; pin the SHA tag for
reproducible deploys.

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
return errors.Join(runErr, closeErr)
```

Contexts are accepted for start, update, signaling setup, announcement, and
shutdown-sensitive operations.
