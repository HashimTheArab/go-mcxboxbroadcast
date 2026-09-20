# Contributor and agent guidance

Keep the README focused on installing, configuring, running, and embedding the
broadcaster. Put maintenance guidance and implementation constraints here.

## Dependencies

Treat `go.mod` as the source of truth for versions and replacements.

- `go-xsapi/v2` owns Xbox Live MPSD/RTA sessions, social APIs, and presence.
  It currently uses the upstream module directly.
- `go-nethernet` owns NetherNet/WebRTC transport. It uses the upstream module,
  which contains the networking changes previously maintained in Lunar's fork.
- `gophertunnel` owns Bedrock protocol handling, signaling, room announcements,
  and session metadata. It currently uses the `HashimTheArab/gophertunnel` fork.
  Before removing the replacement, verify that upstream supports the Xbox
  friend-list NetherNet behavior used here.
- `go-raknet` uses the `HashimTheArab/go-raknet` fork for RakNet ping compatibility.

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
  mutex. Discard stale results and revalidate a sub-account's identity under the
  recovery lock before replacing its session.
- A missing sub-account advertisement must only replace that sub-account.
- Querying the publishing account does not verify another player's permissions
  or ability to join. Keep that limitation clear in operator documentation.
- Do not close and reuse an injected `Config.Signaling` connection during
  recovery. `SignalingFactory` allows recreation. When static signaling prevents
  primary activity recovery, report the failure and preserve the cooldown.

## Session metadata and signaling

Joinability is `joinable_by_friends`, matching MCXboxBroadcast.

Direct WebSocket signaling advertises connection type `3`, with its numeric
network ID in `RakNetGUID`. JSON-RPC Player Messaging advertises type `7` with
`PmsgId` and `NetherNetId`. JSON-RPC cannot share its Player Messaging identity
across independently owned sub-account sessions, so reject that combination
at startup.

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
