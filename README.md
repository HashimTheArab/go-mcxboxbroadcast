# go-mcxboxbroadcast

`go-mcxboxbroadcast` publishes a Minecraft: Bedrock Edition server as an Xbox Live
friend-list world. When a player joins, it relays their join offer to the
server's NetherNet HTTP signaling endpoint and returns the server's answer, so
the player connects to the server directly, as MCXboxBroadcast does.

## Acknowledgements

This project is a Go port inspired by the original
[MCXboxBroadcast](https://github.com/rtm516/MCXboxBroadcast) work and the
[GeyserMC](https://geysermc.org/) ecosystem. Credit goes to the GeyserMC
project and contributors for the Geyser Bedrock listener behavior and
configuration model that this implementation follows.

The library is modelled after
[MCXboxBroadcast](https://github.com/rtm516/MCXboxBroadcast) while using
Go-first building blocks:

- `github.com/df-mc/go-xsapi/v2` for Xbox Live MPSD/RTA session publishing and
  friends.
- `github.com/df-mc/go-nethernet` for the NetherNet HTTP signaling client.
- `github.com/sandertv/gophertunnel`, using the
  `HashimTheArab/gophertunnel` fork, for Xbox signaling, room
  announcements, and `minecraft/p2p`-compatible session metadata.

## CLI

```sh
go run ./cmd/broadcaster -config config.yml
```

If `config.yml` does not exist, the command writes a default one and exits so
you can set `session.joinUrl`; it refuses to start while the target is still
the example host. The first run starts Microsoft device-code
authentication and stores the Live token at `accounts.primaryCachePath`.

Configuration keys use the exact camelCase names shown in
[`config.example.yml`](config.example.yml). YAML and TOML are supported. Unknown
keys, including legacy kebab-case keys and `slack-webhook`, stop startup with an
error naming the key. Older `configVersion`s are migrated and rewritten on load.

Sessions advertise Minecraft 1.26.60 (protocol 2225), like MCXboxBroadcast.
Xbox filters players by this pair before they join, so one bot serves one
version. Set `session.sessionInfo.version` and `protocol` together to advertise
another pair.

Use `-debug` or set `debugMode: true` in the config to show detailed runtime
events such as session creation, presence heartbeats, friend sync scans, pending
friend-request accepts, friends being added/removed, and the final add/remove
counts for each sync pass.

The config exposes the same operator-facing areas as MCXboxBroadcast:

- session target, update interval, query options, broadcast setting,
  world type, displayed MOTD data, and how old sessions are retired
- gallery showcase image upload through `gallery.imagePath`
- friend request acceptance and expiry settings, including last-seen history path
  (stored as JSON at `friendSync.expiry.historyPath`, not Java's SQLite
  database — operators migrating from MCXboxBroadcast start with a fresh
  expiry history)
- Slack/Discord-compatible webhook notifications
- the Xbox account token cache path
- optional HTTP proxy URL through `http.proxy`
- optional Prometheus metrics through `metrics.listen`

### Joining

`session.joinUrl` is the base URL of the server's NetherNet HTTP signaling,
such as `https://play.example.net:19132`. For each join offer the broadcaster
waits for the client's trickled candidates (500 ms after the last one, 3 s at
most), checks `GET {joinUrl}/v1/join`, and posts the complete offer to
`{joinUrl}/v1/join/{connectionId}`. The answer is sent back over Xbox signaling.
If the check fails the client gets connect error 1; if the join request fails
it gets error 6. The broadcaster never accepts the game connection itself, so
the server sees the player's real address and login.

The player's identity token in the offer is verified, then their last-seen
time is recorded for friend expiry. `session.queryServer` reads the same
status document the server serves on `/v1/join`.

### Session rotation

Players stay members of the session while they play, so their friends can find
and join the world too. An Xbox session holds 30 members. At 28 the broadcaster
publishes a new session on the same signaling connection and keeps the old one
for players who are still joining. After 30 seconds the old session is made
invite-only when `session.hideOldSessions` is true (the default), so its
members stop showing it to their friends. Set it to false to keep the
friends-of-friends tag on old sessions. An old session is closed once its grace period has
passed and every player has left it.

### Friends

Minecraft only lets friends join a session, so friend sync works with friends
alone. `friendSync.autoFriend` accepts incoming friend requests, and requests
from accounts whose restrictions prevent a friendship are declined. Expiry
removes friends that have not joined within `friendSync.expiry.days`; the
broadcaster's own accounts are never expired.

Xbox does not publish the friend limit, and it is above 1000. When a player's
own friend list is full, Xbox refuses that one request; their request waits an
hour while everyone else is still accepted. Only when two different players are
refused with no accept since does the broadcaster treat its own list as full and
pause accepting for an hour.

### Session recovery

The broadcaster automatically recovers lost signaling connections, repeated
session-update failures, and missing or closed Xbox activity advertisements.
Activity checks allow time for Xbox to show new sessions and use a cooldown
to avoid repeated restarts. Recovery failures appear in the logs and are sent
to the configured webhook.

These checks use the publishing account's credentials. They detect a missing
directory advertisement, but do not verify friendship permissions or prove that
another player can join. Relayed joins are the end-to-end evidence: alert when
a busy account's joins stop (see [Metrics](#metrics)).

If recovery keeps failing, the command closes its resources and exits with an
error. Run it under Kubernetes or another process supervisor with automatic
restart enabled so the next process recreates authentication and client state.

### Metrics

Set `metrics.listen` (for example `127.0.0.1:9466`) to serve Prometheus
metrics on `/metrics`. The listener starts before sign-in and exports nothing
until the broadcaster has started. The endpoint has no authentication and exposes the
account's XUID, gamertag and friend count, so bind to all interfaces (`:9466`)
only on a network where just your scraper can reach it. It reports:

- `mcxboxbroadcast_session_advertised{xuid,gamertag,version,protocol}`: 1 when
  the last answered check found the account's own session open and checks are
  still running. Xbox errors keep the last answer for up to 15 minutes. It is 0
  until a check first finds the session.
- `mcxboxbroadcast_friends` and `mcxboxbroadcast_friend_limit`: the friend list
  size, counted on the first friend sync and each expiry scan (or, with expiry
  off, on the next friend sync once the count is 30 minutes old), and adjusted
  for adds and removals in between. It is missing until friend sync first runs.
- `mcxboxbroadcast_friend_list_full`: 1 while Xbox refuses friend requests
  because the account's own list is full.
- `mcxboxbroadcast_relayed_joins_total`: join offers relayed since start.

### Signaling

Sessions use Xbox JSON-RPC signaling, the only signaling vanilla clients join
through. Older configs that set `session.signalingMode` or list
`accounts.subAccounts` load with those keys removed; only the primary account
broadcasts.

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

The egg creates `config.yml` on install and exposes the target join URL,
displayed server names, query behavior, notifications, and gallery
settings as panel variables. Account tokens, friend history, and other runtime
state are stored under `/home/container/cache`. On first start, complete the
Microsoft device-code sign-in shown in the console; if notifications are
enabled, the sign-in prompt is also sent to the configured webhook.

## Library

Leave `Config.Signaling` unset to allow automatic primary-session recovery.
Use `SignalingFactory` if you need custom signaling that can be recreated.
Supplying a fixed `Config.Signaling` connection disables primary-session
recreation; a missing activity advertisement then produces a logged error and
webhook notification.

`Broadcaster.Wait()` returns any terminal recovery error. Call `Close()` afterward
to release resources. A normal shutdown returns no recovery error.

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
    JoinURL:              "https://play.example.net:19132",
    HideOldSessions:      true,
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

`Status.Version` and `Status.Protocol` override the advertised pair together;
leave both empty for 1.26.60/2225.

## Development

See [AGENTS.md](AGENTS.md) for dependency guidance, implementation constraints,
and validation commands.
