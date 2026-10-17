# AnyTLS stage evidence prototype

This independent experiment is based on the deployed `61f2efa50563367ee5662b31e0e3ed26cf6a3988` implementation. It is **not deployed**. It adds evidence and a Unix-only diagnostic controller endpoint: no changes to proxy deadlines, protocol framing, padding, authentication, target routing, write quantum, socket settings, or buffering policy.

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

The slow-event queue and terminal-record ring each hold 32 entries. There is one pending batch and one exporter goroutine. Excess batches/events/terminal records increment explicit loss counters. At most 64 session rows (live plus terminal) and 32 slow events are encoded. The complete JSON response body is limited to 64 KiB; the inner JSON publication reserves 1 KiB for the fixed envelope. A larger body becomes a bounded summary with `payload_limited=true`, `omitted_sessions`, and `omitted_slow_events`; missing rows are a coverage gap. Terminal closed/evicted snapshots use `inflight_known=false` and a null active list, not a false claim of zero work. Delayed export reports age measured from capture end until publication begins. The response's `age_ms` includes producer queue/encoding time and time up to the cached snapshot read. It excludes subsequent HTTP serialization/transfer and consumer processing; the isolated consumer separately bounds its request to one second.

Normal DATA operations use only a fixed per-session observation mutex; they do not take the hub registry mutex. Network I/O and export I/O never run while an observation mutex is held. The dedicated exporter serializes outside those mutexes, then atomically replaces one immutable cached body. HTTP callers copy the publication; they neither trigger collection nor hold a session/registry mutex. Slow HTTP readers do not hold up the producer. A failed encoder increments `failed_exports_total` and leaves the previous, aging publication available.

The new stage exporter no longer calls `log.Warnln`, logrus, or the observable log subscriber chain. Its additional shared/Docker log budget is **zero bytes**. This removes this diagnostic feature's dependency on both the unbuffered shared `logCh` (whose observable can block on a full 200-event subscriber queue) and the shared logrus sink. It does **not** fix existing native logging: those existing callers can still block. Changing their global behavior is outside this candidate.

## Unix-only publication contract

`GET /diagnostics/anytls-stage` is mounted only by the Unix controller's router. The TCP/TLS controllers and named-pipe controller continue using the existing public router without this endpoint. It inherits the existing Unix management socket's permissions; this feature does not broaden or fix those permissions. The deployment must preserve its existing private directory/socket (0700/0600). Cookies are private join material and must never be exposed through a reverse proxy or public report.

Before any authenticated session starts the observer, the response is `availability=inactive` with a null snapshot/sequence/age. **Inactive does not distinguish a disabled configuration from an enabled listener that has not authenticated a session (or an initialization failure).** It never asserts zero proxy activity. After creation but before the first sample the state is `awaiting_sample`. Thereafter a ten-second heartbeat publishes `ready` even when no tracked opt-in sessions are active. This is observer state, not a statement of the current configuration of every listener.

Each body/envelope carries a random process-observer `evidence_epoch` and monotonically increasing `sequence`. The body is immutable across reads; `age_ms` measures time since capture at the time of the HTTP read. No history accumulates in the latest-publication slot. A missing/failed/stale response and a skipped sequence are unknown coverage, not zero waits. The fixed snapshot is copied per request; the existing controller's general HTTP concurrency limits are unchanged. The consumer integration must poll once per regular ten-second cycle, never from the one-second TCP burst worker. At that cadence the hard response/output budget is 64 KiB per ten seconds; it is not a new background log stream.

## Private socket association and consumer contract

On Linux, the listener retains its original transport wrapper and reads `SO_COOKIE` through its raw TCP socket after authentication, without changing socket options. TLS `NetConn()` unwrapping is bounded. Unsupported wrappers/platforms and unavailable/closed sockets produce null. There is no fallback that guesses association from nearby timestamps or endpoint similarity.

The raw cookie and random session token are private join material. A future root-only consumer must use the existing collector HMAC key and exact scope (full container ID, container start time, and netns). The collector's cookie key input is `json.dumps([scope, "cookie", canonical_hex_cookie])`; the session token needs its own scoped HMAC. Public reports must omit raw cookies, tokens, addresses, and secrets. A loopback TCP socket was read via `SO_COOKIE` and independently matched to the existing production collector's `ss -e` parser in an isolated temporary result. The public result contains only a success flag and match count.

`docs/experiments/anytls_stage_unix_consumer.py` is an isolated contract fixture, not an installed collector. It has a Unix-only request, one-second timeout, 64 KiB body/output bounds, exact key/enum/type validation, scoped HMAC association, and a fixed one-epoch/sequence cursor. It exports only an explicit whitelist. Repeated sequences do not repeat slow events or cumulative metrics. Missing/stale/invalid input breaks continuity but retains the duplicate high-water cursor; a subsequent repeated old sample is still suppressed. Cursor checkpoints contain only scoped HMAC identity and counters. Restoring a checkpoint preserves deduplication but never assumes continuity across a restart. The production integration must persist it with its record or recover it from the last committed record to avoid crash/replay duplication.

The output is always `diagnostic:anytls_stage` with `fault_count_increment=0`. Native I/O errors and idle header reads remain evidence, not a business failure rate. Metrics are explicitly cumulative; downstream must not sum them as per-interval counters. Snapshot/registry/slow-event truncation and lifetime loss totals remain visible. Samples older than 25 seconds are unavailable to this fixture. The earlier text consumer remains only for historical `[AnyTLS EVIDENCE]` logs; this candidate creates no such log lines. Malformed evidence is a coverage problem and never a proxy failure.

## Acceptance and remaining release gates

**Decision: HOLD for collector integration and enabled-cost/runtime gates.** The 2026-10-17 fast-path revision removed the large default-off regression in the measured write case. The following Unix-publication revision removes the shared-logger dependency; its separate test result is recorded with that revision. This branch is not deployed.

### 2026-10-17 Unix publication and consumer contract

The stage feature now publishes one immutable cached snapshot and is read through the Unix controller only. It adds zero shared-log/Docker-log bytes. Blocked shared log subscribers and a blocked shared logrus sink did not block stage publication in the isolated regressions; native logging behavior remains unchanged.

The first window, **10:16:40.903084872–10:19:45.073864869 UTC**, exited **1**. Its 16 stage-focused session tests passed with race detection in 3.140 s, including verified TLS 1.2/1.3, padding, full DATA/FIN integrity, capacity/privacy and publication concurrency. The Unix route test then failed because its assertion incorrectly expected `json.RawMessage` decoded from explicit JSON `null` to be Go `nil`; the actual decoded value contains the bytes `null`. The listener and consumer steps were not run in this first window. This failed attempt remains recorded.

Only that test assertion and its failure diagnostic changed before the hot retry, **10:22:58.264310537–10:23:44.910215608 UTC**, which exited **0**. Two route tests passed (0.022 s), the actual authenticated TLS listener gate passed (10.030 s), and all nine Python consumer tests passed. The actual Go-generated publication also passed the Python consumer with its cookie association and repeated-sequence suppression. The router tests verify that the public router used by TCP/TLS has no endpoint (404), while the Unix router is read-only with explicit inactive/null state and bounded output. Full CLI/runtime socket exposure and permissions remain a separate gate.

Both windows used Go 1.25.6, one Go worker, 0.35 CPU, 512 MiB RAM, 1024 MiB RAM+swap, a network-isolated container and shared caches under the common experiment lock. No throughput load or production probe was run. The protocol/write fast path was unchanged in this revision, so the previous off-cost experiment was not repeated. Sanitized per-window results are in `docs/results/20261017-anytls-stage-unix-export/acceptance.json`.

### 2026-10-17 nil fast path

The disabled writer no longer creates a `stageWriteOp`, calls observation methods, or registers an observation completion defer. The opt-in entry owns that work. Both entries acquire the same writer mutex and call one shared padding/deadline implementation, so the protocol body is not duplicated. Every disabled transport write directly calls the original connection. Disabled read and pipe wrappers directly perform their original I/O. Enabled observation semantics, authentication gating, capacity limits, and private association fields are unchanged.

The isolated verification ran **2026-10-17 09:45:57.520593541–09:48:16.758987482 UTC** with the same Go 1.25.6, single Go worker, 0.35 CPU and 512 MiB limit, no external network, and shared dependency/build caches. Full session race passed in 34.050 s, including verified TLS, padding/frame integrity, A1 writes/cancellation, and the bounded stage tests. The actual listener default-off/authentication gate passed in 10.024 s. The fixture runs in a network-isolated container; it is not a production probe.

The identical 16 KiB write benchmark was run against released `61f2efa` in ABBA order, five 500 ms measurements per group:

| Variant | All-sample median | Delta from released base | Allocation |
| --- | ---: | ---: | ---: |
| Released `61f2efa` | 2642 ns/op | — | 64 B / 1 allocation |
| Fast-path prototype, default off | 2710 ns/op | +68 ns / +2.57% | 64 B / 1 allocation |

Group medians were 2646, 2547, 2717, and 2621 ns/op respectively. All twenty measurements are retained in `docs/results/20261017-anytls-stage-fastpath/acceptance.json`. This is a limited CPU-quota microbenchmark, not a zero-overhead guarantee, whole-server cost measurement, or demonstrated network speed improvement. The earlier +25.42% result below remains historical evidence from a different run; these figures are not a direct simultaneous old-prototype/new-prototype comparison. No broader performance load was repeated.

The real 60-second dead-peer test and full listener-package race were not repeated; the existing boundaries below still apply. At this fast-path revision, shared logger risk, diagnostic consumer integration, full tagged runtime validation and enabled population/log-volume budget remained gates. The subsequent Unix-publication revision addresses the stage feature's shared logger dependency. No current production configuration or binary was changed.

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

Before production enablement, the remaining concrete gates are:

1. Integrate the Unix consumer into the normal private collector cycle, within its timeout/scheduling budget; preserve and test the cursor across collector restart, socket/container epoch change, missing response, and file rotation. Write a separately bounded private evidence file so stage detail cannot evict the TCP sample budget. No PCS integration is included here.
2. Measure the enabled observer's CPU/RSS, admission/operation mutex cost, JSON/HTTP cost and lost-coverage counters at representative concurrency. The completed default-off microbenchmark does not cover this enabled path. General native logger behavior is unchanged; stage evidence itself no longer uses it.
3. Build and validate the exact full-tagged runtime with the candidate's configuration: Unix permissions, Unix ready/inactive responses, TCP/TLS endpoint exclusion, verified TLS authentication and unchanged protocol rejection/padding behavior. This source-level route test is not that full-runtime acceptance.
4. After these technical gates pass, deploy in a coordinated window with rollback and compare 5/15/30-minute natural observations. These prototype tests do not establish a speed improvement or identify the current production bottleneck.
