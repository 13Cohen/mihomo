# AnyTLS client lifecycle and authenticated listener acceptance

Status: source and pinned candidate image validation completed. Production rollout and observation are recorded separately in PCS operations; this document records isolated acceptance.

## Provenance and scope

Production source remains MetaCubeX/mihomo v1.19.17, `a001b1b11008ba469442e5e59b058dd032263470`. Independent branch `pcs/anytls-client-acceptance` in https://github.com/13Cohen/mihomo is based on A1 `021f39a`, with two separately reviewable commits: lifecycle regressions `dc9e6a6` and their minimal client-only fix `7593b14`. Dependencies and wire protocol are unchanged. The default branch is unchanged. Experiment branch pushes do not match the baseline Alpha/version-tag release workflows.

The client-pool fixes below apply to Mihomo's client. The deployed users use Shadowrocket; these fixes must not be presented as production server improvements or evidence about that closed-source client. The A1 server comparison uses a fixed manually framed authenticated client, so client-pool changes cannot account for its result.

## Client lifecycle results

- A dial completing after Client.Close returned could publish and start a session. The baseline returned a stream with no error and retained one session. The fix checks the client cancellation state under the same sessionsLock that serializes registration with Close, closes the unregistered underlying connection outside the lock, and returns io.ErrClosedPipe.
- Idle cleanup read the skiplist length before taking idleSessionLock. A real Session pool reuse test reproduces races against both insertion and removal. The fix takes the existing lock before reading the length; no I/O is performed under it.
- Baseline runs: 2026-10-15 17:08:20–17:08:42 UTC. Late dial regression fails in 0.071 seconds; pool race regression fails in 0.193 seconds with two race reports. Four concurrent workers complete 48 exact echo streams over four real Session connections using protocol SETTINGS/SYN/DATA/SYNACK/FIN and default padding.
- Candidate run: 17:09:48–17:09:59 UTC. Both regressions pass under the race detector for ten repeats, 480 total echo streams, package duration 1.691 seconds. This fixture uses net.Pipe and does not itself test TLS or password authentication.

Raw synthetic-only outputs are in `docs/results/20261015-anytls-client`. Package durations exclude compilation and are not speed benchmarks.

Reproduction with the same test file copied into the unchanged baseline and candidate:

```sh
go test -mod=readonly -p 1 ./transport/anytls/session -run '^TestClientCloseDuringDialDoesNotPublishSession$' -count=1 -timeout=20s -v
go test -race -mod=readonly -p 1 ./transport/anytls/session -run '^TestClientPoolConcurrentReuseAndIdleCleanup$' -count=1 -timeout=20s -v
go test -race -mod=readonly -p 1 ./transport/anytls/session -run '^TestClient(CloseDuringDial|PoolConcurrent)' -count=10 -timeout=30s -v
```

## Authenticated listener matrix

The independent test file `listener/anytls/acceptance_test.go` exercises the actual listener, real TLS, password authentication and the production sing handler. Candidate ordinary execution passed on 2026-10-15 at 17:45:13–17:45:20 UTC, after cold compilation from 17:39:48–17:45:13. The unchanged production baseline, using the identical fixture file (SHA256 `c3f4317b95118566266017d0fe77cf651b6064f69b27f7f3463a6d51d27bf3c4`), compiled and ran on 2026-10-15 during 18:10:57–18:11:41 UTC. All ordinary matrix cases passed except the deliberate A1 regression: a queued control frame interrupted the progressing authenticated DATA response with `unexpected EOF` after 5.04 seconds. The candidate completed the same full payload, queued control and subsequent stream in 6.127 seconds. These are functional results, not a throughput comparison.

The exact candidate `61f2efa50563367ee5662b31e0e3ed26cf6a3988` then passed the entire session package under the race detector during 18:11:41–18:12:22 UTC, package duration 32.195 seconds. This includes the real TLS/frame/concurrent stream cases, bounded deadline checks, session close with waiting writers, partial write failure, close-error publication and client pool lifecycle regressions. The previously measured opt-in 65-second real dead-peer case was explicitly skipped in this repeat. Full listener-package race instrumentation is a separate pending extension: listener production code is unchanged, while all three changed implementation files are in the session package.

- TLS 1.2 and 1.3, default and negotiated custom padding, four concurrent workers sharing one real client pool and connection reuse with every payload checked.
- Eight concurrent streams on one verified/authenticated outer TLS session, exact full payloads and padding negotiation.
- Two credential identities and correct tunnel metadata; wrong password, wrong hostname and untrusted certificate rejection.
- A paused reader on one connection with healthy traffic on a different connection; payload recovery, expired read and write-entry deadlines, deadline reset, explicit stream read cancellation and canceled fresh dial.
- The fixed manual framing client authenticates to the actual HandleConn path. A 32768-byte DATA response progresses through a paced TCP/TLS writer while another stream's SYNACK queues behind it; full DATA, subsequent control frame and later exact echo are required. Baseline and candidate use the same raw client sequence.

The runner uses Go 1.25.6 with the pinned module versions, network-none test containers, GOMAXPROCS=1, -p=1, CPU 0.35, 512 MiB memory/1024 MiB total memory+swap, and the shared exclusive experiment lock. Dependency prefetch is separate from tests. Production currently reports Go 1.25.4 and the `with_gvisor` build tag. A release must retain that tag and verify full binary/config compatibility; the listener fixture alone does not prove it.

## Pinned old CLI runtime positive control

`docs/experiments/anytls-runtime-smoke.py` passed against the pinned original image on 2026-10-15 at 18:19:11–18:19:14 UTC, after fixture-only corrections for the original safe certificate path policy and the API's initial `connections: null` response. The original CLI configuration validator and actual listener both passed. The synthetic loopback fixture ran with Docker network-none, no published ports, CPU 0.35 and 256 MiB memory. It is a functional smoke test, not a performance measurement.

TLS 1.2 and 1.3 each carried two concurrent streams, with four complete 65536-byte response bodies checked byte for byte; negotiated padding, settings and SYNACK were verified. Two separate user identities were correctly attributed by the Unix API. Wrong password, wrong hostname and an untrusted certificate were rejected. Only the fixture proxy and HTTP target TCP ports were listening; no TCP management listener was present. The controller directory was 0700 and socket 0600, explicitly applied by the harness to mirror deployment policy rather than attributed to Mihomo's default socket creation. Certificate files stayed inside the allowed Mihomo home directory; the test did not widen SAFE_PATHS. `mihomo -v` confirmed the original `with_gvisor` tag.

The exact same script passed the candidate image at 18:33:11–18:33:15 UTC, exit 0: image `sha256:cc4b1b8ece31d5a405bb3d6a5437153df93f7a069cf8c91414973438c2a7a174`, version `v1.19.17-pcsproxy1`, Go 1.25.6 and `with_gvisor`. All payload, concurrency, authentication, padding and controller checks matched the old-image positive control; see `candidate-cli-runtime.json`. Both runs cleaned their isolated containers. A synthetic credential/certificate fixture does not replace the separate read-only production-config compatibility check. No production credentials, destinations or traffic are used by this test.

```sh
python3 docs/experiments/anytls-runtime-smoke.py --image PINNED_IMAGE_ID --output-dir PRIVATE_RESULT_DIRECTORY
```

## Remaining boundaries

The earlier real nonreading TCP/uTLS peer test remains valid: 65.007 seconds total, a timeout error and a closed Session, from a 60-second underlying socket write deadline plus up to five seconds of TLS close_notify. The separate blocked DATA + queued FIN + eight Session.Close regression uses net.Pipe, and all closers and queued writers exit within three seconds; it is not mislabeled as a real-TLS cancellation test. Fixed uTLS v1.8.3 Conn.Close uses activeCall to close the underlying transport directly when Write is in flight, avoiding its outgoing TLS mutex and close_notify in that case.

A1's 60-second total DATA write budget begins after taking the session write lock, is not a rolling progress budget and does not bound queue wait. TLS close_notify may add five seconds. Stream.SetWriteDeadline is checked at Write entry only; Stream.Close can queue FIN behind an active DATA write. The existing Session.Close cancellation test does not prove stronger stream-level semantics.

Mihomo client OpenStream starts a three-second SYNACK watcher on reused version-2 sessions (stream ID at least two). A server writer no longer being interrupted by a queued five-second control deadline does not guarantee a client will tolerate equally long control-frame latency. The fixed raw client intentionally isolates the server defect; this does not characterize Shadowrocket behavior.

The client stream dieHook is assigned after OpenStream publishes the stream. Concurrent remote FIN/Session.Close is still a separate unproven hypothesis; the passing pool runs do not establish all lifecycle code is race-free. A canceled context while reusing an already idle session is not covered by the fresh-dial cancellation test.

The separate receive HOL diagnostic on branch `pcs/receive-hol-experiment` proves a server receive loop can wait in synchronous stream.pipeW.Write and delay same-session upstream DATA/control parsing while another outer session remains healthy. It does not establish the cause of observed server-to-client SendQ stalls. There is no per-stream receive window in this wire protocol. A bounded staging queue could absorb a transient pause but cannot guarantee indefinite isolation at saturation without dropping/terminating or changing the protocol. Such a prototype must separately specify per-stream/session/process byte and frame limits, ownership/release of pooled buffers, DATA-before-FIN order, cancellation cleanup and explicit saturation behavior; it is excluded from this candidate.

Production acceptance still needs real traffic windows, sufficient denominators, stage/error accounting, latency, TCP progress/retransmissions, CPU/RSS and reconnects. Synthetic acceptance demonstrates a concrete defect is removed in that scenario, not a universal speedup or proof it caused a live incident.

## Pinned build and rollout gate

After the listener matrix and reviewed timeout policy pass, build from the exact accepted source commit with the production feature set retained:

```sh
GOMAXPROCS=1 GOOS=linux GOARCH=amd64 GOAMD64=v1 CGO_ENABLED=0   go build -mod=readonly -p 1 -tags with_gvisor -trimpath   -ldflags '-X github.com/metacubex/mihomo/constant.Version=v1.19.17-pcs-a1-ACCEPTED_SHA -w -s -buildid='   -o out/mihomo .
./out/mihomo -v
```

Use the pinned Go 1.25.6 toolchain already used in the regressions, record its exact image ID, source SHA and binary SHA256, and confirm `with_gvisor` appears in the binary version output. The root-coordinated fixed-archive build passed at 18:23:06.943250–18:32:25.403976 UTC using Go 1.25.6, CGO_ENABLED=0, GOAMD64=v1 and with_gvisor, with no dependency upgrades. The resulting binary SHA256 is `0ff9f06a4132aec3ed94e42c1e0f783ada9879e41a5ba28f4026742c3e2088ef`. The isolated CLI smoke independently confirmed the version/tag and actual listener behavior. This does not exercise a live TUN interface. The existing runtime image ID is `sha256:652d3933868d439771c3286e982dec1a59ca889a0704a79576db5c85f2bd1490`; preserve that exact image as rollback and create the candidate by replacing only `/mihomo` in a new image based on the same runtime. Do not rebuild from floating upstream alpine/latest or refresh geodata as an unrelated change.

Before a canary, verify candidate configuration parsing against read-only deployment configuration, version/tags, private controller permissions, authentication/TLS/padding and an isolated real-client smoke test. The collector and rollback image must survive the single-service transition. The accepted implementation candidate is exactly `61f2efa50563367ee5662b31e0e3ed26cf6a3988`. Relative to production, its only changed non-test Go files are `transport/anytls/session/session.go` (A1 deadline ownership and partial-write close), `stream.go` (atomic immutable close-error publication), and `client.go` (two independently reviewed pool lifecycle fixes). Client-pool fixes are retained in this exact tested candidate; they are not called by this server listener path and are not attributed as server or Shadowrocket performance gains.
