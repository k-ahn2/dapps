# DAPPSv1 session traffic optimisations - PROPOSAL

## TL;DR

A captured exchange between G5ALF-3 and M0AHN-3 (AGW, 1200 baud, wps-repl
traffic) moved **11 messages of ~200 bytes in 77 s over 6 separate AX.25
connections, using about 150 frames**. Most of that is overhead:

- **One connection per message.** The forwarder opens a fresh session for
  every queued message, even when several are waiting for the same peer.
- **Four turnarounds per message.** `ihave` → `send` → `data`+payload → `ack`,
  at roughly 1 s per turnaround on this link.
- **An extra frame per message.** `data <id>\n` and the payload are sent as
  two I-frames.

Proposals 1-3 need no wire-protocol change and would bring the same exchange
down to 1-2 connections and about 85 frames. Adding 4 and 5 gets it to about
40 frames.

All code references are to `c6b62e8` (0.39.0).

## The captured exchange

| Observation | Where in the trace |
|---|---|
| G5ALF had sn=22, 23 and 24 queued by ~08:11:03 (worked back from `ttl=`), but sent each in its own session | Sessions at 08:11:26, 08:11:33, 08:11:48 |
| Each session costs SABM/UA, `DAPPSv1>`, `rev`, a closing `DAPPSv1>` and DISC/UA (about 8 frames) before any payload moves | Every session |
| The 2 s gap between DISC and the next SABM | `LinkSettleGate`, 2 s default |
| `data <id>` and the payload go out as separate transmissions, with an RR in between | e.g. 08:11:03 / 08:11:04 |
| Bare RRs are sent even when the same station's I-frame follows within a second | e.g. 08:11:02 `RR R1` then `I S0 R1` |
| 3 of the 11 messages are wps-repl `{"op":"ack"}`; one of them needed a whole session to itself | 08:12:12 |
| `ihave` header is ~150 bytes for a 203-byte payload | Every offer |
| `rev` with nothing queued still costs a round trip | 08:11:53, 08:12:02 |

The two code paths behind most of this:

- `OutboundMessageManager.DoRunCore` loops over pending messages and calls
  `ForwardAndObserveAsync` once each (`OutboundMessageManager.cs:85`, `:152`).
- `Dappsv1SessionBackhaul.SendAsync` takes a single `BackhaulMessage` and does
  connect → prompt → offer → data → `routes` → `rev` → disconnect for it.

## Proposals

Ranked by benefit for effort. "Protocol" says whether peers need to change.

### 1. Batch all pending messages for a next hop into one session

**Change.** In `DoRunCore`, group `NextHop` decisions by route callsign and
pass the list to a batched `SendAsync(route, IReadOnlyList<BackhaulMessage>)`
that connects once, loops offer/data/ack, then does `routes` and `rev` once.

**Benefit.** In the trace, G5ALF's 5 sessions become 1: about 36 frames and
~30 s of airtime saved. Each extra message in a batch costs only its own
exchange, not a new connection. It also means fewer chances for two nodes to
dial each other at once (the #178 crossed connect), and `LinkSettleGate`
rarely has a redial left to hold back.

**Protocol.** None. The receiver already accepts repeated `ihave` on one
session, and its `rev` drain already sends several messages per session.

**Must handle.**
- Record outcomes per message (routing feedback, `MarkMessageAsForwarded`,
  metrics), so messages already acked stay marked as done if the link drops
  mid-batch.
- A crossed connect at the prompt, or `PeerSessionBusyException`, defers the
  whole batch: no failure, no cooldown.
- The per-destination backoff and the `WouldDialIntoOpenSession` check move
  from per message to per batch.
- Flood copies can be batched per neighbour too (second priority).

### 2. Check the queue again before disconnecting

**Change.** After the `rev` drain, if messages for this peer were queued
during the session, offer them before DISC. Optionally, keep the link open
for a configurable idle time (10-20 s) before disconnecting.

**Benefit.** Covers messages that arrive while a session is open, which
batching alone misses. In the trace, the reconnect 2 s after each DISC would
have been avoided.

**Protocol.** None.

**Must handle.** While we hold the session, `PeerSessionRegistry` makes the
peer's forwarder defer anything it has for us, so its mail only reaches us
through our `rev`. Send `rev` again just before disconnecting, not only once
after pushing.

### 3. Write the `data` line and payload in one call

**Change.** Build one buffer and call `WriteAsync` once in
`DappsProtocolClient.SendMessageAsync` (`DappsProtocolClient.cs:228-229`).
The inbound `rev` drain uses the same method, so both directions are fixed.

**Benefit.** One I-frame and one key-up saved per message, and often an RR
too. That's 11 frames and 11 key-ups in the trace. A 203-byte payload plus
the 13-byte `data` line fits inside a 256-byte PACLEN.

**Protocol.** None. About 3 lines of code.

**Why keep the `data` line.** It's redundant in the current strict flow, but
it marks where the unframed payload bytes start. It also lets the receiver
reject stray text (node banners after a link reset, errors) rather than
reading `len` bytes of garbage. And it's what makes pipelining (4a) work.
It costs 13 bytes and no turnaround.

### 4. Pipeline offers, or send small payloads straight after `ihave`

**Change.** Two options, both behind capability negotiation (e.g. a
`DAPPSv1.1>` prompt or a `caps` command) so older peers are unaffected:

- **4a. Pipelining.** Send N `ihave` lines in one burst, read N `send`/`no`
  replies, send all the data, read N acks.
- **4b. Payload with the offer.** When `len` is below a threshold, send
  `ihave` + `data` + payload without waiting for `send`.

**Benefit.**
- 4a: four turnarounds per *batch* instead of per message, and the AX.25
  window (MAXFRAME up to 7) gets used properly.
- 4b: two turnarounds per message instead of four. The payload is sometimes
  wasted when the receiver already has the message (flood duplicates), but at
  ~200 bytes that's cheaper than a 1 s turnaround.

**Protocol.** Yes, negotiated.

### 5. Cumulative, delayed wps-repl acks (app side)

**Change.** Wait a few seconds, then send one ack for the highest contiguous
seq per origin. Better still, carry it on outgoing replication data.

**Benefit.** The separate acks for seq 48 and 50 in the trace become one, sent
in an existing session. That removes the 6th session entirely. Ack traffic
grows with bursts rather than with every message.

**Protocol.** None for DAPPS; the change is in wps-repl.

### 6. AX.25 tuning (operator docs)

**Change.** Document in `docs/tune.md`:
- Raise T2/RESPTIME so the node waits for the app's reply and carries the
  ack in it, not in a bare RR.
- Raise MAXFRAME so that, once 1 and 4 are in, several I-frames go out per RR.

**Benefit.** Removes most standalone RRs. Mostly pays off after 1 and 4.

**Protocol.** None; node configuration only.

### 7. Shrink the `ihave` header (later, versioned)

**Change.** Options:
- Drop fields that are at their defaults (`gt=0`, `fmt=p`).
- Encode `s=` and `ttl=` more compactly.
- Once batching exists, compress all payloads in a batch together
  (`fmt=d`). Compressing each 200-byte JSON message on its own gains little.

**Benefit.** The header is ~75% of payload size for small messages;
trimming it is the largest remaining per-message byte saving.

**Protocol.** Yes, negotiated.

### 8. Skip `rev` when nothing is waiting (minor)

**Change.** The callee says how many messages it has queued for the caller,
in its `ack` line or prompt. The caller skips `rev` when there are none.

**Benefit.** One round trip per session when the peer has nothing queued
(sessions 4 and 5 in the trace).

**Protocol.** Yes, negotiated.

## Expected effect on the captured exchange

These are estimates from the trace, not measurements.

| | Connections | Turnarounds per message | Approx. frames |
|---|---|---|---|
| Today | 6 | 4 | ~150 |
| 1 + 2 + 3 | 1-2 | 4 | ~85 |
| + 4a + 5 | 1 | ~1 (per batch of 4) | ~40 |

## Suggested order

1. **3** - trivial, no protocol change, immediate saving.
2. **1**, then **2** - the largest saving, no protocol change.
3. **5** - raise with wps-repl.
4. **6** - docs, once 1 is in.
5. **4**, **7**, **8** - together, behind one capability negotiation.
