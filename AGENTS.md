# Contributor and agent guidance

Keep the README focused on installing, configuring, running, and embedding the
broadcaster. Put maintenance guidance and implementation constraints here.

## Dependencies

Treat `go.mod` as the source of truth for versions and replacements.

- `go-xsapi/v2` owns Xbox Live MPSD/RTA sessions, social APIs, and presence.
  It currently uses the upstream module directly.
- `go-nethernet` owns the NetherNet HTTP signaling client (`endpoint`) that
  relays join offers. Use it rather than hand-rolling `/v1/join` requests.
- `gophertunnel` owns Xbox signaling, room announcements, session metadata, and
  multiplayer token verification. It currently uses the
  `HashimTheArab/gophertunnel` fork. Before removing the replacement, verify that
  upstream supports the Xbox friend-list NetherNet behavior used here.
- `go-raknet` is only an indirect dependency; its replacement matches the one
  gophertunnel's fork builds against.

## Session lifecycle

`session_recovery.go` owns the loop that serializes updates and full recovery.
Keep recovery in that loop so concurrent paths cannot bypass retry backoff or
tear down a replacement session. Recovery pauses normal updates and has bounded
retries; exhaustion must reach `Broadcaster.Wait()` so the CLI can exit and its
supervisor can restart it. See the constants there for retry limits and delays.

`session_activity.go` checks directory advertisements separately from metadata
updates. An unchanged announcement can return successfully from the local cache
without contacting Xbox. Preserve these checks when changing recovery:

- Match the publishing owner and current session with `SessionReference.Equal`.
  Session names and template names are case-insensitive.
- Require consecutive successful lookups that find no open matching handle.
  Request errors reset the miss count and must not trigger recovery.
- Keep a grace period for a new publication and a per-account cooldown that
  survives session replacement. Timing constants live in `session_activity.go`.
- Bound the total lookup time and make HTTP requests outside the broadcaster
  mutex. Discard stale results and revalidate the session's identity under the
  recovery lock before replacing it.
- Querying the publishing account does not verify another player's permissions
  or ability to join. Keep that limitation clear in operator documentation.
- Do not close and reuse an injected `Config.Signaling` connection during
  recovery. `SignalingFactory` allows recreation. When static signaling prevents
  primary activity recovery, report the failure and preserve the cooldown.

## Joins and session rotation

`relay.go` never accepts the game connection. It gathers trickled candidates
(500 ms quiet, 3 s cap), probes `GET /v1/join`, posts the merged offer, and
returns the answer or a connect error (1 for a failed probe, 6 for a failed
join). Player history is only recorded after the offer's identity token and
fingerprint signature verify.

Players stay session members while they play. Every member counts toward the
30-member cap, so a session rotates at 28 members: the new session reuses the
signaling connection and the old one is kept, then hidden after 30 seconds when
`HideOldSessions` is set. The session loop closes a retired session once its
grace period has passed and only the bot remains, so retired sessions do not
pile up RTA subscriptions. A failed primary rotation falls back to full recovery.
These thresholds mirror MCXboxBroadcast; keep them in sync with it.

Friend sync manages friends only. Do not reintroduce follower handling.

## Session metadata and signaling

Joinability is `joinable_by_friends`, matching MCXboxBroadcast.

Sessions advertise JSON-RPC signaling, connection type `7` with `PmsgId` and
`NetherNetId`; vanilla clients join through no other signaling. Its Player
Messaging identity belongs to one account, so the broadcaster publishes from a
single account; there are no sub-accounts.

## Validation

Run focused regression tests for the behavior being changed. For lifecycle,
recovery, or concurrency changes, also run:

```sh
go test ./...
go test -race ./...
go vet ./...
```

Use the existing HTTP test adapters and `testing/synctest` for directory failures,
grace periods, and retry timing. Keep tests independent of live Xbox accounts.
Documentation-only edits need link and diff checks, not another full test run.
