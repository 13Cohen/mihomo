# AnyTLS stage evidence prototype

This independent experiment is based on the deployed `61f2efa50563367ee5662b31e0e3ed26cf6a3988` implementation. It is **not deployed**. It adds evidence only: no changes to deadlines, protocol framing, padding, authentication, routes, write quantum, socket settings, or buffering policy.

The next decision needs to separate a stream waiting for the shared session writer from the writer actually waiting in TLS/TCP, and distinguish an incomplete received frame from a complete frame waiting for the application reader. A1 removes the erroneous control deadline override; it does not add fairness or remove these waits. The earlier receive HOL reproduction concerns server ingress. It does not establish the cause of observed server egress SendQ.

## Enablement and observation semantics

The optional listener field `stage-evidence` defaults to false. The normal server listener attaches evidence only after its existing authentication and initial padding validation. Client sessions reject enablement. The default path creates no hub, observer goroutines, timers, or observation state. It retains nil-handle checks, whose cost is measured separately against the exact released source.

| Stage | Meaning | Important limit |
| --- | --- | --- |
| `writer_wait` | A submitted write is waiting for `Session.connLock` | The snapshot carries the oldest tracked wait and total pending count, not all queued writes |
| `writer_owned` | The caller owns that writer lock | Includes padding/buffering and transport writes; it is not added to TLS duration as independent time |
| `tls_write` | A `net.Conn.Write` call is in progress | The production connection wraps TLS; the time also includes wrapper and observation-boundary overhead, not only kernel blocking |
| `read_header` | Waiting for a complete protocol header | An idle healthy session normally waits here; this is not an error |
| `read_body` | Header was received but its complete body has not been read | Does not prove network loss or identify a particular peer-side cause |
| `pipe_write` | A full received DATA frame waits for application consumption | This is server ingress dispatch and can hold up the session's receive loop |

`frame_class` on a writer/TLS operation classifies the submitted `writeConn` call. Existing buffering and padding may combine frames or add padding; it is not a claim about every frame inside the resulting TLS record. No stream ID, user, target, IP, or payload is emitted. Byte counts are sizes only. Error counters describe API returns, including cancellation/close; they are not a business failure rate.

Snapshots run every 10 seconds. They include a real capture start/end UTC range and a separately captured time for each session row. In-flight ages use a fresh clock reading after the slot is locked, never a potentially stale scheduled ticker time. Completed operations retain cumulative count/total/max/error/slow counters. A bounded queue captures operations lasting at least 100 ms, including `started_at`, `completed_at`, and a monotonic duration, so a completed 3–6 second wait need not coincide with a periodic snapshot to leave evidence. These records can still be lost at the explicit capacity limits; absence never establishes absence of stalls.

## Fixed capacities, coverage, and isolation

The process-wide hub contains 32 fixed session slots, with 32 tracked waiter slots per session. A new authenticated session prefers a vacant slot; when full, admission rotates. This avoids permanently describing only the first 32 sessions, but **is not an unbiased or complete population sample**. The batch reports live authenticated sessions, tracked/untracked live sessions, evictions, lost completions, and waiter overflow. Evicted handles have generation checks and cannot write into a reused slot.

The slow-event queue and terminal-record ring each hold 32 entries. There is one pending batch and one exporter goroutine. Excess batches/events/terminal records increment explicit loss counters. At most 64 session rows (live plus terminal) and 32 slow events are encoded. JSON exceeding 64 KiB becomes a bounded summary with `payload_limited=true`; missing rows are a coverage gap. Terminal closed/evicted snapshots use `inflight_known=false` and a null active list, not a false claim of zero work. Delayed export reports age measured from capture end until sink invocation. That age does not include time subsequently spent inside the sink.

Normal DATA operations use only a fixed per-session observation mutex; they do not take the hub registry mutex. Network I/O and log/export I/O never run while an observation mutex is held. The dedicated exporter may block and the queue may drop evidence without making the observer calls wait for that sink.

This isolation does **not** make all Mihomo logging nonblocking. `log.Warnln` still uses the existing unbuffered `logCh` and shared logrus sink. A slow sink can affect other existing native log callers. This prototype does not change that logger. Its production enablement requires a separate log-path/backpressure review; the blocked-sink unit test establishes only the observer's isolation and bounded loss behavior.

## Private socket association and consumer contract

On Linux, the listener retains its original transport wrapper and reads `SO_COOKIE` through its raw TCP socket after authentication, without changing socket options. TLS `NetConn()` unwrapping is bounded. Unsupported wrappers/platforms and unavailable/closed sockets produce null. There is no fallback that guesses association from nearby timestamps or endpoint similarity.

The raw cookie and random session token are private join material. A future root-only consumer must use the existing collector HMAC key and exact scope (full container ID, container start time, and netns). The collector's cookie key input is `json.dumps([scope, "cookie", canonical_hex_cookie])`; the session token needs its own scoped HMAC. Public reports must omit raw cookies, tokens, addresses, and secrets. A loopback TCP socket was read via `SO_COOKIE` and independently matched to the existing production collector's `ss -e` parser in an isolated temporary result. The public result contains only a success flag and match count.

The sample consumer in `docs/experiments/anytls_stage_consumer.py` is an isolated contract fixture, not an installed collector. It validates the schema and size bounds, performs the scoped HMAC association, and classifies `[AnyTLS EVIDENCE]` as `diagnostic:anytls_stage` **before** generic warning classification. The existing production collector would otherwise count this warning as `warning:other`; therefore consumer integration is a release gate. Malformed evidence is a coverage problem and must not increment proxy failure totals.

## Acceptance and remaining release gates

**Decision: HOLD for the remaining integration and enabled-cost gates.** The 2026-10-17 revision removes the large default-off regression in the measured write case. Functional and relevant race checks pass. This branch is ready for further integration review, not production enablement.

### 2026-10-17 nil fast path

The disabled writer no longer creates a `stageWriteOp`, calls observation methods, or registers an observation completion defer. The opt-in entry owns that work. Both entries acquire the same writer mutex and call one shared padding/deadline implementation, so the protocol body is not duplicated. Every disabled transport write directly calls the original connection. Disabled read and pipe wrappers directly perform their original I/O. Enabled observation semantics, authentication gating, capacity limits, and private association fields are unchanged.

The isolated verification ran **2026-10-17 09:45:57.520593541–09:48:16.758987482 UTC** with the same Go 1.25.6, single Go worker, 0.35 CPU and 512 MiB limit, no external network, and shared dependency/build caches. Full session race passed in 34.050 s, including verified TLS, padding/frame integrity, A1 writes/cancellation, and the bounded stage tests. The actual listener default-off/authentication gate passed in 10.024 s. The fixture runs in a network-isolated container; it is not a production probe.

The identical 16 KiB write benchmark was run against released `61f2efa` in ABBA order, five 500 ms measurements per group:

| Variant | All-sample median | Delta from released base | Allocation |
| --- | ---: | ---: | ---: |
| Released `61f2efa` | 2642 ns/op | — | 64 B / 1 allocation |
| Fast-path prototype, default off | 2710 ns/op | +68 ns / +2.57% | 64 B / 1 allocation |

Group medians were 2646, 2547, 2717, and 2621 ns/op respectively. All twenty measurements are retained in `docs/results/20261017-anytls-stage-fastpath/acceptance.json`. This is a limited CPU-quota microbenchmark, not a zero-overhead guarantee, whole-server cost measurement, or demonstrated network speed improvement. The earlier +25.42% result below remains historical evidence from a different run; these figures are not a direct simultaneous old-prototype/new-prototype comparison. No broader performance load was repeated.

The real 60-second dead-peer test and full listener-package race were not repeated; the existing boundaries below still apply. The shared logger risk, diagnostic consumer integration, full tagged runtime validation and enabled population/log-volume budget remain release gates. No current production configuration or binary was changed.

### 2026-10-15 initial prototype result

The original prototype was held specifically for its default-off cost regression; its measurements remain below for traceability.

The final isolated window was **2026-10-15 19:06:46.018640449–19:11:26.195246311 UTC**, with Go 1.25.6, `GOMAXPROCS=1`, 0.35 CPU, 512 MiB RAM, 1024 MiB RAM+swap, and no external network. Full session race tests passed in 34.174 s; the real listener authentication gate passed in 10.024 s. Three Python consumer tests and the existing collector's socket-cookie association passed. An earlier initial window timed out during listener dependency compilation after its session tests passed; that entire window is explicitly not recorded as a pass. All windows and individual benchmark samples are retained in the result artifact.

The same 16 KiB DATA-to-discard connection fixture was run in ABBA order (released base, candidate, candidate, base), three 500 ms benchmark samples per group:

| Variant | All-sample median | Delta from released base | Allocation |
| --- | ---: | ---: | ---: |
| Released `61f2efa` | 2932.5 ns/op | — | 64 B / 1 allocation |
| Prototype, default off | 3678.0 ns/op | +745.5 ns / +25.42% | 64 B / 1 allocation |

This was an in-memory write microbenchmark under CPU quota, not TLS or public-network throughput. It identified a cost concern rather than a measured user-speed regression. The separate enabled inner-loop comparison had medians 3406 ns/op off and 5032 ns/op on; it excludes periodic JSON and log-sink work. The subsequent nil-path revision and new comparison are recorded above; this original result has not been relabeled as a pass.

The checked-in tests cover verified TLS 1.2/1.3 with normal padding, a paced DATA writer plus queued FIN, exact resulting DATA/FIN bytes, incomplete frame body versus full-frame pipe wait, completion maxima and slow events, actual slow-event timestamps, delayed-ticker capture times, export queue age, authenticated listener enablement/default-off/bad credentials, cookie availability, registry/waiter overflow, generation reuse, explicit unknown state after close, blocked exporter, concurrent snapshot/admission, and diagnostic consumer privacy/classification.

Machine-readable sanitized results and the exact limited-resource test windows are in `docs/results/20261015-anytls-stage/`. The final session race run also reruns A1 and client-pool regressions. The optional real 60-second dead-peer test is not repeated: no deadline behavior changes here, and its earlier acceptance remains documented separately. A full listener-package race build is not claimed; the real listener gate is tested without race, and the modified session package is tested with race.

Before any production enablement: review measured off/enabled overhead; resolve the shared logger risk; integrate the consumer without counting diagnostic warnings as failures; verify the full tagged runtime/config path; establish a resource/log volume budget under representative concurrency; and then use a staged observation window with real traffic. These prototype tests do not establish a speed improvement or identify the current production bottleneck.
