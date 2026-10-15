# Next AnyTLS concurrency acceptance, after A1

Design only, 2026-10-15. This document does not change the frozen release candidate `61f2efa50563367ee5662b31e0e3ed26cf6a3988`, dependencies, production configuration or client settings.

## Priority and existing evidence

The first target is **server write-side scheduling between complete frames**. The authenticated listener regression already proves that a progressing DATA write holds the session writer while another stream's SYNACK waits. A1 removes the wrong writer's five-second deadline overwrite; it does not make that SYNACK arrive sooner. Production has previously shown server-to-client SendQ and retransmission, which is the relevant direction, but no recorded stage timing yet proves that writer scheduling caused those live incidents.

At the fixed candidate, `session.go:408–487` holds connLock across the complete TLS write and padding work, `session.go:371–386` submits one DATA frame per Stream.Write, and `stream.go:169–176` submits server SYNACK through that same writer. `session.go:141–149` is the separate Mihomo client three-second SYNACK watcher; we keep clients fixed in comparisons, rather than asking users to change it. Shadowrocket's corresponding policy is unknown.

The second target is **server receive dispatch**. The independent `pcs/receive-hol-experiment` branch at `11b48b0182a2437d9945eb78b1dcfcb8f3e1c10f` already distinguishes complete-frame dispatch blocking from an incomplete wire frame, with same-session and healthy cross-session controls. `session.go:185–198` reads a complete DATA frame, releases the stream-map read lock, then waits synchronously in pipeW.Write. This is client-to-target direction at this server. It cannot explain a server-to-client SendQ observation by itself.

## What the existing collector can and cannot distinguish

The current collector provides outer-session stream-count distributions, non-atomic API-to-outer association coverage, TCP SendQ/RecvQ/notsent, ACK/delivery/retransmission deltas and bounded one-second bursts. These can distinguish visible kernel backpressure with or without ACK progress, and help establish whether multiple streams share one outer connection. They cannot identify the owner of connLock, time spent waiting for it, time inside TLS Write, or time a complete inbound frame waits for its application reader. Small request stalls can also fall below burst triggers. Empty queues do not prove the application is unblocked.

## Minimal stage evidence before changing scheduling

Use an opt-in diagnostic extension on a separate source branch, with fixed-size counters/histogram bins and no payload, credential, destination or raw endpoint logging. Avoid global locks on DATA paths.

| Stage | Measurements | Interpretation |
| --- | --- | --- |
| Session writer acquisition | frame class, bytes, pending writers, queue duration | Same-session scheduling wait before owning the socket |
| Owned writer | TLS write duration, successful/partial bytes, timeout/error class, active frame class | Backpressure or TLS work while holding the writer; correlate with socket progress |
| Receive header/body | time and frame class; distinguish incomplete header/body | Waiting for the ordered wire stream, not application dispatch |
| Complete-frame dispatch | pipeW.Write duration, pending bytes, reader resumed/closed | Application-reader wait after all frame bytes have arrived |

A completion-only slow-event log misses an operation that never completes. Add an atomic current-stage/start-time snapshot per live Session and a bounded periodic exporter, plus completed-event counters. Per-session storage must be fixed-size. Export at most 256 active session rows every ten seconds, with total counts and explicit omitted-row counts. A process-random opaque session identifier can join stages without revealing addresses. To join the existing API outer groups, expose that opaque identifier as a dedicated optional connection metadata field populated by this listener; do not overload usernames or target fields. Any missing/non-atomic mapping remains unknown, not zero wait.

A bounded asynchronous slow-event channel may record waits of at least 100 ms: at most one detailed event per session per second and 60 per process per minute, with explicit dropped-event totals. Logging must never block the writer or receive loop; aggregate counters continue when detailed events are dropped. Export only through the existing private Unix controller or a root-only bounded local log. Measure disabled/enabled allocation, CPU and write latency overhead before deployment. No new remote management listener or unbounded goroutine/map is needed.

## First behavior experiment: bounded DATA frame quantum

Compare exact A1 against server-side complete-frame quanta of 16 KiB and 8 KiB, initially in an isolated fixture. Split only at protocol frame boundaries and release the session writer between complete frames. Never interleave bytes inside an existing frame or TLS write. Do not change the client uploader, authentication, certificate verification, protocol commands or negotiated padding policy. The present server constructor does not enable outbound padding; preserve that fact rather than claiming newly added server padding. Verify both default and custom inbound padding and exact decoded output.

Chunking changes frame/record sizes and timing; compatibility and the existing padding policy can be tested, but identical traffic-analysis characteristics cannot simply be assumed. A frame quantum also cannot bypass bytes already queued in the same ordered TCP stream or recover lost TCP segments. It is a scheduling candidate, not a cure for arbitrary network loss.

Start by extending the existing authenticated paced-writer fixture. Keep the same manually framed client and same server handler; add a fixed Mihomo-client variant on both sides of the comparison to expose its existing SYNACK policy without attributing client changes to the server. Require a slow bulk stream, at least seven small concurrent streams, queued SYNACK/FIN, same-session control latency, exact DATA ordering and a separate-session healthy control. Record each stage, rather than aggregate throughput alone.

| Scenario | Necessary check |
| --- | --- |
| Slow but progressing bulk plus small streams on one session | Small-stream/control latency improves while all payloads and FIN order remain exact |
| Four independent outer sessions, two credential identities | No global scheduling lock or cross-user starvation; correct attribution |
| Receiver temporarily pauses, then resumes | Finite queue ownership and exact recovery; report whether the kernel or application blocked |
| Permanently dead peer, concurrent Session.Close | Original bounded convergence and cancellation remain valid |
| Partial write/timeout between frames | Never reuse a damaged partially transmitted frame; returned byte count is accurate |
| TLS 1.2/1.3 and default/custom padding | Authentication and verification rejects unchanged, complete frame decoding valid |
| Zero, small, 65535, 65536 and larger application Writes | Correct length encoding and chunk boundaries; no silent uint16 length wrap |

The last row also investigates a static boundary: current writeDataFrame converts len(data) to uint16 without visibly splitting larger input. Ordinary routing buffer sizes may prevent this in present production paths; it is not yet a measured production defect. Test it explicitly before generalizing the chunk implementation.

After deterministic functional/race acceptance, use isolated fixed-delay/loss/receiver-pause cases and ABBA ordering, the same toolchain, CPU/RAM and socket settings. Begin with a 160 ms path and both one/four outer connections; do not change tcp_notsent_lowat at the same time. Record small-response/control P50/P95, full-payload success, disconnects, bulk goodput, CPU/RSS and per-session fairness. Predeclare a meaningful control-tail improvement and a bulk/CPU regression guard after fixture calibration; do not cherry-pick the best quantum or confuse a unit-test delay with Internet performance. Change system send-buffer/backpressure settings only in a later joint matrix after A1 correctness is retained.

## Separate receive prototype, only if evidence warrants it

A bounded per-stream staging FIFO can absorb a transient reader pause, but the inspected protocol has no per-stream receive-window command. At saturation, the only compatible lossless default is to backpressure the shared receiver again; indefinite isolation would require an explicit termination policy or protocol/client change. Do not silently drop DATA, return a normal FIN before queued DATA, spawn one unbounded goroutine per frame, or treat a larger buffer as removal of this limitation.

A prototype must first specify and test stream/session/process byte and frame caps, bounded workers, buffer ownership, cancellation release and saturation behavior. Repeat the existing complete-frame versus incomplete-wire diagnostic with actual TLS/listener authentication and a slow target reader. Check byte-for-byte order across DATA then FIN, healthy cross-session traffic, no leaks after close and precise cap enforcement. This remains separate from downlink writer fairness and from the current release.
