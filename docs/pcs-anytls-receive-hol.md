# Receive dispatch isolation experiment

Status: fixture and acceptance design prepared; **not yet compiled or executed**.
No production behavior, collector, network parameter or binary change is part
of this experiment. The experiment branch is `pcs/receive-hol-experiment`, based
on the separate A1 source experiment and tests at 021f39a; the receive loop itself
remains identical to deployed a001b1b.

## Question and direction

At a001b1b, `transport/anytls/session/session.go:196` calls
`stream.pipeW.Write(buffer)` synchronously inside the only recvLoop for that
session. `transport/anytls/pipe/io_pipe.go` waits for a reader to consume the
frame before the write returns. The map lookup lock is released first, so this
is a per-session dispatch dependency, not a globally held stream-map mutex.

On the server this path dispatches **client-to-target input**. The recent live
observation instead showed substantial **server-to-client output** queuing and
retransmission. The receive-path hypothesis is not established as that incident's
cause. A different client's receive implementation also cannot be inferred from
Mihomo's source. Outer-stream concurrency evidence is needed to determine how
many unrelated streams actually share one session in use.

Two different waits must be separated:

1. The outer ordered TCP/TLS byte stream has not delivered the remainder of an
earlier frame. A receiver cannot parse later frame boundaries yet. Dispatch
buffering cannot remove this transport/framing constraint.
2. A complete earlier DATA frame is already available and read, but delivery to
its application reader blocks the receive loop before it examines later DATA,
heartbeat or FIN frames. This is an implementation dispatch dependency that
buffering could soften within explicit resource limits.

## Self-contained fixture

`transport/anytls/session/receive_hol_experiment_test.go` drives actual Session
SETTINGS, SYN, DATA, heartbeat and FIN parsing. A synthetic net.Conn holds a
complete ordered frame script and reports exactly how many bytes recvLoop has
consumed. It accepts server control writes and detects the actual heartbeat
response frame. No listener authentication, real TLS, DNS, routing, target
network or payload logger is involved. All addresses and payloads are synthetic.
The fixture therefore isolates Session behavior rather than measuring Internet
latency or proving the full server free of global contention.

A slow stream receives 4096 patterned bytes; an unrelated stream carries a
separate marker. The latter's completion requires both intact DATA and its FIN.
A separate Session with both readers ready is the healthy control. Releasing
only the slow application reader must restore exact payload/FIN delivery.
A separate wire-gated case exposes only half of the first DATA frame while both
application readers are ready; releasing the missing bytes must restore delivery.
Session.Close must also cancel a receive loop waiting on a stream pipe.

| Case | Baseline hypothesis, not a measured result yet | Acceptance purpose |
| --- | --- | --- |
| Complete slow DATA, paused application reader, same-session other DATA/FIN and heartbeat | Other DATA/FIN and heartbeat wait | Isolate application dispatch HOL after complete frame arrival |
| Separate Session while the first session is blocked | Intact DATA/FIN still complete | Check isolation at the Session layer, not the listener/router stack |
| Incomplete preceding wire frame with both application readers ready | Later DATA and heartbeat cannot proceed | Preserve framing; do not confuse transport HOL with application dispatch |
| Release the slow reader or missing wire bytes | All expected bytes and FINs complete, no corruption | Verify resumption and ownership/order of pooled frame buffers |
| Session.Close while receive dispatch is blocked | recvLoop and streams terminate | Ensure cancellation and future queue-worker cleanup remain bounded |

The default diagnostic test reports whether same-session progress occurred,
without treating current blocking as a desirable permanent contract. Setting
`PCS_ANYTLS_REQUIRE_RECV_ISOLATION=1` makes progress of the unrelated DATA/FIN and
heartbeat an explicit future-candidate acceptance gate. Baseline failure of that
gate would be expected evidence, not a test-infrastructure success or a reason
to hide the failure. The 100 ms observation period is a controlled pause, not a
latency target for the Internet.

## How to validate when the shared experiment slot is free

Keep the deployed source/dependencies and toolchain constant. Copy only this
self-contained test file to the a001b1b baseline worktree; do not change baseline
implementation. Compilation and tests must use the shared experiment lock and
resource cap. These commands are planned, not recorded as already run:

```sh
go test -mod=readonly -p 1 ./transport/anytls/session -run '^TestReceive' -count=1 -timeout=30s -v
PCS_ANYTLS_REQUIRE_RECV_ISOLATION=1 go test -mod=readonly -p 1 ./transport/anytls/session -run '^TestReceiveSlowReaderDiagnostic$' -count=1 -timeout=30s -v
CGO_ENABLED=1 go test -race -mod=readonly -p 1 ./transport/anytls/session -run '^TestReceive' -count=10 -timeout=30s -v
```

The same source can run against the A1 branch because receive dispatch has not
changed there. A green default diagnostic on A1 must not be reported as removal
of receive HOL: compare the logged progress booleans and the explicit gate.

## Fix options and constraints

A small per-stream FIFO with one ordered writer could absorb transient reader
pauses. It must transfer/copy ownership of pooled DATA buffers before recvLoop
returns them, preserve per-stream frame order, drain received DATA before a FIN
becomes visible, cancel workers and return buffers on every error/close path,
and bound bytes **and** frames at stream, session and process scopes. Spawning
an unbounded goroutine for each pipe.Write would risk reordering, buffer reuse
corruption and unbounded memory/goroutines; it is not an acceptable fix.

The inspected protocol frame set has no explicit per-stream receive-window
command. Consequently bounded, lossless buffering cannot guarantee indefinite
isolation when one reader never drains: when its allowance fills, the server
must either pause the shared receiver or introduce a reviewed policy to terminate
that stream/session. Silent DATA drops or falsely reporting normal FIN completion
are not acceptable. An additional flow-control protocol extension would require
client compatibility work. The current server-only experiment must not pretend
otherwise.

A future queue prototype should first target transient pauses within a specified
byte budget, retain an explicit saturation policy, and demonstrate useful gains
without increasing failures. Required follow-up gates are:

- Intact ordered payloads across mixed frame sizes and multiple streams, including
queued DATA followed by FIN, peer resets and simultaneous close.
- Slow but progressing reader, permanently dead reader, queue saturation and
cancellation; finite bounds on queue memory, frame count, worker lifetime and
session teardown.
- Fast-stream DATA/FIN and heartbeat latency with one slow stream, compared with
baseline and a separate-session control. Do not count aggregate throughput alone
as a fairness improvement.
- TLS 1.2/1.3, default and negotiated padding, listener authentication, actual
client/session reuse and full race checks after a candidate exists.
- Production canary only after acceptance gates pass, with outer-stream mapping,
TCP progress, stage timing and rollback evidence. No live benefit is claimed now.

This receive experiment also does not replace A1's send-deadline work or justify
changing tcp_notsent_lowat: earlier send backpressure can expose the old control
frame deadline bug more often. Those changes require their own combined checks.
