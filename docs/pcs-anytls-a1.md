# AnyTLS A1: deadline ownership acceptance experiment

Status: source experiment only. No production binary deployment.

## Provenance

- Upstream: https://github.com/MetaCubeX/mihomo
- Independently maintained source: https://github.com/13Cohen/mihomo
- Baseline: v1.19.17, `a001b1b11008ba469442e5e59b058dd032263470`, the deployed source revision.
- Candidate implementation: `e8b73c2447c365bd8c992c0d1c66b8ecf279f2d3`, directly based on that baseline.
- Experiment branch: `pcs/control-deadline-experiment`. Default `Meta` is unchanged.
- The user repository is a public independent mirror, not a GitHub fork-network member. Its v1.19.17 tag exactly matches upstream, and the baseline is an ancestor of its Meta branch. This experiment does not rebase onto current Meta.
- The baseline workflows trigger builds/releases only for Alpha or version tags (or manual dispatch). Pushing this experiment branch does not trigger those workflows.

Tests use Go 1.25.6, the original module versions including `github.com/metacubex/utls v1.8.3`, and `-mod=readonly`. Both baseline and candidate use the same test toolchain. Production is still its existing Go 1.25.4-built binary. No dependency or protocol version was upgraded.

## Behavior under test

The baseline control-frame writer sets a connection-wide five-second deadline before acquiring the shared write lock. A FIN queued behind a progressing DATA write can therefore interrupt the DATA write. The candidate gives the write-lock owner exclusive deadline set/clear ownership; DATA has a finite 60-second whole-write budget and a control write has five seconds. A failed DATA write closes the session so a partial frame cannot be followed by another frame on the same connection.

The 60 seconds starts after acquiring the lock and covers one complete `writeConn`, including any accumulated buffer and padding. It does not bound time waiting for that lock and is not a rolling no-progress detector or a session lifetime. Even a slowly progressing write exceeding 60 seconds will be terminated. This is an experimental policy requiring further review, not an accepted production tuning value.

The underlying uTLS close path can add another five seconds for `close_notify`: `github.com/metacubex/utls@v1.8.3/conn.go`, `Conn.Close` and `closeNotify` (the latter sets a five-second write deadline). A real nonreading TLS peer exited after 65.007 seconds in this fixture. Therefore an application writer's observed total return time is not claimed to be 60 seconds. Concurrent explicit Session.Close can interrupt a blocked write; the queue is not allowed to hold a dead socket indefinitely.

## A1 fixtures and results, 2026-10-15

All TLS fixtures use real loopback TCP, the same uTLS server dependency as the production listener, and ephemeral ECDSA certificates whose trust chain and hostname are verified. Socket buffers are 4096 bytes. Slow-progress fixtures pace 512-byte underlying TCP writes by 100 ms after the handshake. They exercise actual TLS record generation; they are deterministic regression tests, not Internet speed benchmarks.

| Check | Baseline a001b1b | Candidate e8b73c2 |
| --- | --- | --- |
| TLS 1.2 and 1.3 client writer, buffered SETTINGS + DATA + padding + other-stream FIN + post-padding DATA | Both fail around 5.02 s with I/O timeout | Both complete around 6.03 s; all six full frames and every payload byte verified |
| TLS 1.2 and 1.3 actual server writer, DATA + other-stream FIN | Both fail around 5.03 s with I/O timeout | Both complete around 6.04 s; complete DATA and FIN verified |
| Sixteen concurrent streams, DATA then FIN | Pass | Pass; all 32 complete frames checked and per-stream order preserved |
| Blocked DATA, queued control, eight concurrent Session.Close calls | Pass | Pass; writers and Close finish within the test bound |
| Partial DATA frame write error | Session incorrectly remains open | Session closes and cannot be reused |
| Real TCP/TLS peer never consumes application records | Not run as a comparable finite DATA policy does not exist | Pass: actual 60 s write timeout plus bounded TLS close, 65.007 s total |
| Concurrent exported Stream.Write and Stream.Close | Race detected at stream.go:56 read vs :87 write | Same existing race detected; A1 not yet fully accepted |

The first candidate package run passed in 30.956 seconds, excluding cold compilation. The baseline comparative subset intentionally failed in 20.118 seconds on the four TLS deadline cases and the partial-DATA close assertion. Ordinary full-frame and Session.Close tests passed on both. The opt-in real-time deadline plus exported stream cancellation run passed in 65.029 seconds.

## Reproduction

Use the same test files with the exact baseline source and candidate source. Do not interpret baseline expected failures as successful acceptance.

```sh
go test -mod=readonly -p 1 ./transport/anytls/session -count=1 -timeout=3m -v
PCS_ANYTLS_REAL_DEADLINE=1 go test -mod=readonly -p 1 ./transport/anytls/session -run TestRealTLSDeadPeerWriteBudget -count=1 -timeout=90s -v
CGO_ENABLED=1 go test -race -mod=readonly -p 1 ./transport/anytls/session -count=1 -timeout=3m -v
```

The real-time test is explicitly opt-in because it deliberately waits about 65 seconds; ordinary package tests skip it. Test traffic is local and bounded to 8 MiB maximum for that dead-peer fixture. Tests must not be run as a production load test. This host's experiment runner serializes compilation and load work with an exclusive lock and caps containers to 0.6 CPU, 768 MiB RAM and 1280 MiB total RAM plus swap, with GOMAXPROCS=1 and Go `-p 1`.

## Remaining acceptance gates

1. Fix and revalidate the pre-existing stream close-error race. The exported Stream implements net.Conn, whose methods may be called concurrently. sync.Once serializes the two close paths but does not synchronize the unsafely read dieErr field in Write. Identical -race tests fail on baseline and candidate; the internal Session lock, frame and cancellation cases do not report a race in this run.
2. End-to-end listener/client authentication and failure paths, negotiated padding behavior, client/session reuse and Shadowrocket interoperability. These package tests use real TLS but do not cover complete client authentication or the deployed client.
3. Choose a reviewed DATA timeout policy. Assess very low rates, cancellation, per-stream deadlines, queued-control latency and memory/goroutine cleanup. A 60-second whole-write budget does not by itself guarantee acceptable latency or fairness.
4. Only then build a pinned candidate for an isolated protocol test. Production canary, if authorized in a later stage, must preserve authentication/TLS/padding and have an exact rollback image.
5. Production acceptance needs matched traffic windows with sufficient real AnyTLS traffic: session/error denominators, timeout stage, DATA progress and bytes, stream duration, first-byte and completion latency, TCP retransmissions/RTT, RSS/CPU and reconnects. Current production observations are too sparse to claim a speed or failure-rate gain. Synthetic success proves this regression is addressed in the fixture, not that it caused a live incident.

Raw private production logs, credentials and remote endpoint information do not belong in this repository.
