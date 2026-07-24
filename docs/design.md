# flight Design Plan

Status: implementation started. This document defines the intended product,
technical shape, and delivery plan for a DuckDB extension for Arrow Flight, and
tracks where the current Rust MVP intentionally falls short of the final shape.

## Executive Summary

`flight` should make remote Arrow Flight datasets feel native inside
DuckDB. A data-inclined engineer should be able to install the extension, list
available flights, inspect a schema, and query a stream in a few minutes without
learning the full Flight protocol first.

The implementation should start from the DuckDB Rust extension template, but we
should treat that template as a foundation to validate, not an assumption to
trust blindly. The template is explicitly experimental today, so Phase 0 must
prove that Rust can support the table-function hooks, async stream lifecycle,
packaging, and performance envelope this extension needs. If it cannot, the
fallback should be a thin C++ DuckDB table-function layer calling a Rust Flight
core.

The first release should be read-only, streaming-first, and modest: metadata
discovery, schema inspection, and `DoGet` scans over path, command, and ticket
inputs. Flight SQL, write APIs, broad nested type support, advanced auth, and
multi-endpoint parallelism should follow only after the read path is pleasant and
measurably fast.

## Current Implementation Status

Working now:

- Rust extension scaffold from the DuckDB Rust extension template.
- Local unsigned extension build through `make configure debug`.
- SQLLogic smoke tests through `make test_debug`.
- Rust unit tests through `cargo test`, including a tiny in-process Flight
  service that serves real Arrow Flight `FlightData` streams over gRPC.
- `flight_check`, `flight_list`, `flight_info`, `flight_schema`,
  `flight_scan`, friendly `flight`, and `flight_scan_ticket`.
- `fixture://events` demo data, `fixture://seattle-weather` real sample rows
  from the public Vega Seattle weather dataset, and
  `fixture://palmer-penguins` public non-human biology rows for offline
  development.
- The Seattle weather sample is also streamed through the in-process Flight
  server in Rust tests, so real-world rows exercise the Arrow Flight decode and
  scan-state path.
- The Palmer penguins sample is streamed through the same in-process Flight
  server path to cover biological measurement data without human privacy risk.
- Basic path and command descriptors, ticket scans, bearer token auth,
  explicit authorization headers, max gRPC message size, and connect/RPC
  timeout control.
- Binary-safe endpoint tickets: descriptor scans preserve opaque ticket bytes,
  `flight_info` exposes `ticket_hex`, and explicit ticket scans accept
  `ticket_encoding := 'utf8' | 'hex' | 'base64'`.
- Schema-aware ticket scans via `schema_descriptor`, allowing tickets to stream
  when the caller can provide a schema lookup descriptor.
- Sequential multi-endpoint scan behavior is covered by an in-process Flight
  server that partitions one dataset across multiple tickets.
- Offline SQLLogic tests use `fixture://...` shortcuts so extension load and SQL
  shape are deterministic without a background service.
- Release identity is `flight` across Cargo, Makefile, CI workflow, docs, and
  the community descriptor template.

Known MVP gaps:

- Descriptor scan functions bind output columns from `FlightInfo` and stream
  endpoint batches during execution. Ticket scans do the same when supplied a
  `schema_descriptor`; bare ticket-only scans still collect batches during bind
  because a bare ticket does not expose a schema to DuckDB.
- Projection/filter pushdown, cancellation, parallel endpoint scheduling,
  benchmark harnesses, Flight SQL, advanced TLS options, and broad nested Arrow
  type coverage remain future phases.
- Error messages are serviceable but not yet as diagnostic or charming as the
  product should be.

## Additional Review Lenses

This round applies two additional lenses without treating either as a literal
impersonation:

- Arrow ecosystem lens: honor Arrow as a shared memory, streaming, and
  interoperability substrate. Avoid building a narrow DuckDB-only tunnel when
  the same choices could make the extension fit naturally with Flight SQL, ADBC,
  Arrow IPC, and other engines.
- DuckDB founder lens: keep the extension small, unsurprising, and fast. Reuse
  DuckDB concepts where users already have expectations, avoid heavyweight
  connector machinery, and make the common case a single SQL function with clear
  failure modes.

## Review Stance

This version incorporates three reviews:

- Staff engineer review: sharpen execution boundaries, risk gates, type mapping,
  async bridging, cancellation, backpressure, tests, and performance proof.
- CTO review: define release criteria, sequencing, staffing assumptions, and
  scope controls so the project can be funded and shipped.
- Cursory user review: make the first-five-minutes path obvious and ensure the
  SQL surface reads like something a DuckDB user would actually try.

## User Promise

The extension should make this path boring in the best way:

```sql
INSTALL flight FROM community;
LOAD flight;

SELECT * FROM flight_list('grpc://localhost:5005');

SELECT *
FROM flight_schema('grpc://localhost:5005', ['datasets', 'events']);

SELECT *
FROM flight('grpc://localhost:5005', ['datasets', 'events'])
LIMIT 100;
```

When auth is needed, the user should still stay in SQL:

```sql
SELECT *
FROM flight(
  'grpc+tls://flight.example.com',
  ['warehouse', 'events'],
  bearer_token := getenv('FLIGHT_TOKEN')
)
LIMIT 100;
```

The lower-level functions should remain available for precision, but the common
path should be short, memorable, and documented first.

For a lakehouse engineer, the promise is slightly more specific: use DuckDB for
local analytical work, use native DuckDB lakehouse extensions when directly
reading Iceberg, Delta, Lance, DuckLake, or Parquet is best, and use
`flight` when the data boundary is a governed Flight service, remote
compute engine, secure data product, or distributed result stream.

## Modern Data Lake Positioning

`flight` should complement DuckDB's native lakehouse extensions, not
compete with them. DuckDB already has strong native paths for table formats and
object-store files. Flight is valuable when the user should not, cannot, or does
not want to read the underlying files directly.

Best-fit use cases:

- Governed data products exposed by a platform team.
- Remote query engines that return Arrow streams.
- Cross-cloud or private-network datasets where object-store credentials should
  stay server-side.
- Large result sets where endpoint partitioning and streaming matter.
- Interactive exploration against services that already expose Flight or Flight
  SQL.
- Hybrid workflows that pull a remote curated slice into DuckDB for local joins,
  feature engineering, export, or validation.

Poor-fit use cases:

- Direct local or object-store scans where `read_parquet`, `iceberg`, `delta`,
  `lance`, or `ducklake` already give DuckDB full control.
- Workloads that need transactionally consistent writes through DuckDB in v0.1.
- Services that use application-specific descriptors but provide no discovery,
  schema, or examples.

Adoption strategy:

- Lead docs with "query a Flight service from DuckDB", not protocol vocabulary.
- Include one local demo server and one Flight SQL demo path.
- Include examples showing `CREATE VIEW AS SELECT * FROM flight(...)`.
- Document when to choose native DuckDB lakehouse extensions instead.
- Align option names with ADBC Flight SQL where practical so users can transfer
  knowledge from Python, Go, JDBC, and other Arrow clients.
- Provide a compatibility matrix for Flight, Flight SQL, auth, TLS, endpoint
  behavior, and supported Arrow types.

## Marketing And DevEx Review

The project needs a sharper public story than "DuckDB extension for Arrow
Flight." That description is accurate, but it undersells the use case. The
README should sell the feeling: remote governed data, local DuckDB questions,
and no connector ceremony.

Positioning:

- Tagline: "Query Arrow Flight services from DuckDB."
- One-liner: "The data lives elsewhere, but the question still feels local."
- Audience: data engineers, analytics engineers, platform teams, and curious
  DuckDB users who already understand SQL.
- Category: service-backed data access for DuckDB, adjacent to lakehouse
  extensions rather than replacing them.
- Tone: technically serious, short, and a little playful.

README requirements:

- State implementation status at the top, including any MVP limitations.
- Show the intended happy path before any architecture discussion.
- Include `flight_check(...)` as the first debugging affordance.
- Include copy-pasteable examples for unauthenticated local use and bearer-token
  TLS use.
- Explain "when to use Flight" and "when to use native DuckDB scans."
- Keep protocol explanation brief and link deeper design docs.
- Avoid overpromising zero-copy, pushdown, Flight SQL, writes, or community
  extension availability before they exist.

Developer experience requirements:

- A future `make demo` should start a local Flight server and open a DuckDB shell
  with examples ready to run.
- A future `make test` should run SQLLogic and Rust tests without requiring a
  remote service.
- A future `make bench` should report latency to first row, throughput, and
  bounded-memory behavior.
- Fixtures should include intentionally awkward cases: expired endpoints,
  unsupported fields, auth failure, TLS failure, and interrupted streams. Binary
  endpoint tickets are already covered by loopback Flight tests.
- Contributor docs should separate "extension plumbing" from "Flight client
  core" so new contributors can find a tractable first issue.
- Error messages should include a suggested next command when possible, usually
  `flight_check(...)` or `flight_schema(...)`.

## Brand And Logo Direction

The project has an obvious and useful pun: ducks taking flight. Use it, but keep
it light. The brand should feel like a precise data tool with a little lift, not
a novelty project.

Brand idea:

- Name: `flight`
- Tagline: "Query Arrow Flight services from DuckDB."
- Secondary line: "Stop waddling files around. Let Flight carry the batches."
- Alternate stickers: "Pond to prod, one SQL query at a time" and "Airborne
  batches, grounded SQL."
- Mood: fast, compact, helpful, slightly playful.
- Avoid: aviation cliches, busy mascots, cartoon excess, or anything that looks
  unrelated to DuckDB and Arrow.

Logo concept:

- Primary mark: a simple duck silhouette or duck head integrated with an upward
  arrow/flight path.
- Data cue: one wing or trail can be made of three clean columnar bars, record
  batches, or stream segments.
- Motion cue: a small ascending path from lower-left to upper-right.
- DuckDB cue: keep the duck shape friendly and compact; do not copy official
  DuckDB artwork unless licensing and brand guidance allow it.
- Arrow cue: use a subtle arrow shape or stream direction, not a literal Apache
  Arrow logo copy.

Logo variants:

- Horizontal lockup: icon plus `flight`.
- Square icon: duck-plus-arrow mark for package registries, docs, and social
  previews.
- Monochrome mark: works in black, white, and one-color terminal contexts.
- Tiny mark: recognizable at 16x16, even if the data bars disappear.

Suggested palette:

- Duck yellow as a small accent, not the whole identity.
- Deep ink or charcoal for text and outlines.
- Arrow blue or sky blue for the flight path.
- Optional green accent for "ready/connected" states in docs or diagrams.

Acceptance criteria:

- Looks credible next to DuckDB and Arrow ecosystem projects.
- Reads clearly at README-header size and favicon size.
- Does not require gradients, tiny details, or complex illustration to work.
- Can be redrawn as SVG with simple paths.
- Has a transparent-background version.
- Includes alt text: "flight extension logo, a duck-shaped mark taking flight along
  an upward arrow path."

## Product Goals

- Make Arrow Flight data feel native in DuckDB SQL.
- Optimize for exploration first, then production repeatability.
- Preserve Arrow's columnar streaming behavior through DuckDB scans.
- Keep memory bounded by stream and batch size, not remote dataset size.
- Expose Flight service capabilities instead of hiding them behind magic.
- Make errors actionable around auth, TLS, descriptors, endpoints, tickets,
  schemas, stream interruption, and unsupported types.
- Provide a clean path toward Flight SQL without making Flight SQL a v0.1
  dependency.
- Be fun enough that users reach for it during ad hoc analysis, not only inside
  carefully engineered pipelines.
- Help lakehouse teams expose governed, high-throughput data services without
  forcing every analyst to manage cloud object-store credentials locally.

## Non-Goals

- Do not reimplement a general Flight server.
- Do not require a custom Flight protocol dialect.
- Do not chase full Flight SQL coverage in the first MVP.
- Do not add write support until read semantics, auth, and performance are
  stable.
- Do not add a broad connection manager before table-function state proves what
  reuse is needed.
- Do not promise zero-copy until the DuckDB/Rust/Arrow bridge proves it.
- Do not parse Iceberg, Delta, Lance, DuckLake, or catalog metadata inside this
  extension. Let native DuckDB extensions and Flight services own those layers.

## Success Metrics

Release candidates should publish numbers, even if they are rough:

- First query: a new user can list flights and scan 100 rows in under 10 minutes
  from the README.
- Latency: time to first row is within 2x a direct Rust Flight client for the
  same service on local loopback.
- Throughput: primitive-column scans sustain at least 70 percent of direct
  Arrow Flight client throughput before DuckDB-side filtering.
- Memory: scan memory remains bounded for datasets larger than available RAM.
- Reliability: interrupted streams, auth failure, missing descriptors, and
  unsupported types all produce specific DuckDB errors.
- Packaging: unsigned local extension load works on macOS arm64 and Linux amd64
  before any broader release claim.
- Adoption: README examples cover local demo, bearer-token service, Flight SQL
  service, and "when to use a native DuckDB lakehouse extension instead."
- DevEx: a contributor can run local demo, tests, and benchmarks without access
  to a private Flight service.

## Design Principles

- SQL first: every feature should have an obvious SQL affordance.
- Discovery before scan: users should be able to inspect services before pulling
  data.
- Friendly top layer, explicit lower layer: `flight(...)` should feel easy,
  while `flight_scan(...)`, `flight_scan_ticket(...)`, and metadata functions
  remain debuggable.
- Fast path by default: scans should stream `RecordBatch` data and avoid
  materializing complete results in memory.
- Pushdown is a contract: projection and filters should be translated only when
  the behavior can be explained and tested.
- Rust owns Flight complexity: async gRPC, Arrow IPC, TLS, auth, and client
  lifecycle should live behind a small DuckDB-facing layer.
- DuckDB owns SQL ergonomics: table functions should fit DuckDB conventions for
  binding, init, projection pushdown, filter pushdown, and SQLLogic tests.
- Interop beats cleverness: where ADBC, Flight SQL, Arrow IPC, or DuckDB already
  establish names and behavior, prefer familiarity over invention.

## Proposed SQL Surface

Recommended public surface for v0.1:

```sql
-- Friendly path descriptor scan.
SELECT *
FROM flight('grpc://localhost:5005', ['datasets', 'events']);

-- Explicit path or command descriptor scan.
SELECT *
FROM flight_scan(
  'grpc://localhost:5005',
  descriptor_type := 'path',
  descriptor := ['datasets', 'events']
);

-- Explicit ticket scan.
SELECT *
FROM flight_scan_ticket(
  'grpc://localhost:5005',
  ticket := '...'
);
```

Metadata and discovery:

```sql
SELECT * FROM flight_list('grpc://localhost:5005');

SELECT *
FROM flight_info(
  'grpc://localhost:5005',
  descriptor_type := 'path',
  descriptor := ['datasets', 'events']
);

SELECT *
FROM flight_schema('grpc://localhost:5005', ['datasets', 'events']);

-- Validate connectivity, auth, TLS, and basic service behavior.
SELECT * FROM flight_check('grpc://localhost:5005');
```

Function contracts:

- `flight(location, descriptor, options...)`: friendly path-descriptor scan.
- `flight_scan(location, descriptor_type, descriptor, options...)`: explicit
  descriptor scan.
- `flight_scan_ticket(location, ticket, options...)`: explicit ticket scan.
- `flight_list(location, options...)`: list advertised flights.
- `flight_info(location, descriptor_type, descriptor, options...)`: expose
  endpoints, tickets, schema metadata, and total record/byte hints when present.
- `flight_schema(location, descriptor, options...)`: return one row per field.
- `flight_check(location, options...)`: diagnose connectivity, auth, TLS,
  supported RPCs, and version/capability hints.
- `flight_actions(location, options...)`: later metadata function.
- `flight_sql(location, query, options...)`: later Flight SQL function.

The user-facing docs should lead with `flight(...)`. Internal development can
still implement `flight_scan(...)` first and add `flight(...)` as a thin alias
before the first public release.

## Result Schemas

The scan functions return the remote Arrow schema mapped into DuckDB logical
types at bind time. Metadata functions should have stable schemas:

- `flight_list`: `descriptor_type`, `descriptor`, `endpoint_count`,
  `total_records`, `total_bytes`, `ordered`, `app_metadata`.
- `flight_info`: `endpoint_index`, `location`, `ticket`, `ticket_hex`,
  `ticket_size`, `expiration_time`, `total_records`, `total_bytes`,
  `app_metadata`.
- `flight_schema`: `field_index`, `field_name`, `arrow_type`, `duckdb_type`,
  `nullable`, `metadata`, `supported`.
- `flight_actions`: `action_type`, `description`.
- `flight_check`: `check_name`, `status`, `message`, `detail`.

Unsupported Arrow fields should be visible in `flight_schema`. Scan functions
should fail clearly if an unsupported field is selected, and they should allow a
projection that excludes unsupported fields when the underlying stream permits
it.

## Options Model

Target API:

- Prefer named parameters for normal use.
- Allow an `options := {...}` struct or JSON string only if the Rust extension
  API cannot expose named options cleanly in the first pass.
- Keep the documented target stable even if the first implementation has a
  lower-level escape hatch.

Candidate options:

- `headers`: request headers.
- `authorization_header`: explicit authorization header, aligned with ADBC
  Flight SQL naming.
- `bearer_token`: bearer token for simple auth.
- `tls_root_certs`: custom PEM roots.
- `tls_override_hostname`: override for certificate validation, aligned with
  ADBC Flight SQL naming.
- `tls_skip_verify`: development-only escape hatch that should warn loudly and
  be disabled in examples unless testing localhost.
- `timeout_ms`: connect and RPC timeout.
- `max_message_size`: maximum accepted gRPC message size.
- `batch_size`: requested batch sizing when the service supports it.
- `parallel_endpoints`: whether multiple endpoints may scan concurrently.
- `app_metadata`: opaque request metadata.
- `allow_unsupported_columns`: permit scans when unsupported columns are not
  projected.

Security rules:

- Do not display secrets in result rows, logs, or error strings.
- Prefer DuckDB secrets integration once the extension has a stable API.
- Treat `getenv(...)` examples as development ergonomics, not the final
  production credential story.
- Validate tokens or auth metadata on every RPC where the server expects it;
  avoid connection-state assumptions that break behind gRPC load balancers.

## Internal Architecture

Planned components:

- DuckDB extension entrypoint: registers metadata and scan table functions.
- Bind layer: validates SQL arguments, creates Flight descriptors or tickets,
  fetches schema or flight info, and maps Arrow schema to DuckDB logical types.
- Flight client layer: owns tonic channels, TLS, auth headers, deadlines, and RPC
  construction.
- Runtime bridge: drives async tonic streams from DuckDB's synchronous scan API.
- Stream layer: converts Flight data streams into Arrow `RecordBatch` values and
  tracks endpoint/ticket progress.
- DuckDB bridge: appends Arrow batch values into DuckDB `DataChunk` output with
  projection-aware materialization.
- Pushdown planner: maps DuckDB projected columns and supported filters into
  Flight descriptors, Flight SQL queries, or application metadata when safe.
- Diagnostics layer: normalizes gRPC, Arrow IPC, auth, TLS, and schema errors.
- Test harness: launches a small local Flight server for SQLLogic and Rust tests.

Initial flow:

```text
User SQL
  -> DuckDB table function bind
  -> Flight GetFlightInfo or GetSchema
  -> Arrow schema to DuckDB types
  -> DuckDB scan init
  -> Flight DoGet streams
  -> Arrow RecordBatches
  -> DuckDB DataChunks
```

Key architecture decisions to prove in Phase 0:

- Rust-only extension vs C++ shim plus Rust core.
- One Tokio runtime per process vs per connection vs per scan.
- Blocking scan thread vs background async producer with bounded channel.
- Direct Arrow-to-DuckDB vector filling vs Arrow C Data Interface bridge.
- Per-scan client construction vs connection reuse.
- How DuckDB cancellation interrupts in-flight gRPC calls.
- How `FlightInfo.ordered`, endpoint expiration, connection reuse locations, and
  endpoint locations map into DuckDB scan scheduling.

Recommended initial shape:

- Start with a Rust core crate that can be tested without DuckDB.
- Keep DuckDB-facing code thin and replaceable until table-function capability is
  proven.
- Use a bounded producer/consumer bridge for `DoGet` so slow DuckDB consumers do
  not create unbounded buffering.
- Treat multi-endpoint parallelism as opt-in until ordering, backpressure, and
  cancellation semantics are tested.
- Preserve Flight protocol concepts internally instead of flattening them too
  early. Descriptors, tickets, endpoint locations, expiration, and ordered flags
  should survive long enough to produce good diagnostics.

## Protocol Reliability Plan

Flight's reliability model is part of the user experience. The extension should
make protocol semantics visible and conservative:

- Respect `FlightInfo.ordered`. Sequential endpoint scanning is the default for
  ordered results; unordered results may use parallel endpoint scanning later.
- Treat tickets as opaque bytes. Provide text helpers for convenience, but keep a
  binary-safe path before public release.
- Support `arrow-flight-reuse-connection://?` and empty endpoint locations by
  reusing the metadata connection.
- Track endpoint expiration and avoid retrying expired tickets.
- Retry only idempotent setup calls by default. Retrying a partially consumed
  `DoGet` should require an unexpired endpoint and a restart from a known-safe
  boundary.
- Surface `UNAUTHENTICATED`, `UNAUTHORIZED`, `NOT_FOUND`, `TIMED_OUT`,
  `CANCELLED`, `UNAVAILABLE`, and `UNIMPLEMENTED` as distinct DuckDB errors.
- Add cancellation hooks early so `LIMIT`, interrupted queries, and client
  disconnects do not leave abandoned server work.
- Keep a diagnostics table or trace mode for endpoint index, selected location,
  batches read, rows read, bytes read, retry count, and final status.
- Support `PollFlightInfo` after the MVP for long-running lakehouse queries that
  produce endpoints incrementally.

## DuckDB Rust Template Plan

Use the Rust extension template for:

- Standard extension layout and loadable extension entrypoint.
- `cargo`-driven Rust library code.
- DuckDB extension packaging conventions.
- SQLLogic tests once the SQL surface exists.
- CI jobs for lint, test, package, and extension install smoke tests.

Template risks to retire before implementation:

- The template is experimental, so do not assume community-extension readiness.
- Confirm the available DuckDB Rust crate table-function API supports bind data,
  init state, projection pushdown, filter pushdown, replacement scans if needed,
  and clean error propagation.
- Confirm macOS arm64 and Linux amd64 builds before expanding the platform
  matrix.
- Confirm whether current DuckDB version support should track the template's
  pinned dependency or the newest stable DuckDB release.
- Confirm how extension signing or unsigned local loading affects early users.
- Confirm whether the API can expose a `flight_check` diagnostic table function
  without pulling in the full scan path.

## Arrow Flight Dependency Plan

The Rust `arrow-flight` crate is the natural client dependency, but its feature
flags matter:

- Keep the first dependency set small: `arrow-flight`, `arrow-array`,
  `arrow-schema`, `arrow-ipc`, `tonic`, and `tokio`.
- Choose one TLS feature intentionally, likely `tls-native-roots` for local
  ergonomics or `tls-webpki-roots` for reproducible packaging.
- Keep `flight-sql` disabled until Phase 6 unless a spike shows it is cheap and
  low-risk.
- Enable IPC compression features only when test fixtures and servers exercise
  compressed Flight payloads.
- Pin Arrow crate versions together and document upgrade cadence.
- Use ADBC Flight SQL option names as a reference vocabulary even when this
  extension talks directly to `arrow-flight`.

## MVP Scope

The first implementation milestone should be intentionally narrow:

- Read-only Flight `DoGet`.
- `flight`, `flight_list`, `flight_info`, `flight_schema`, and `flight_scan`.
- `flight_check` for early supportability.
- `flight_scan_ticket` if ticket handling is straightforward after
  `flight_info`.
- Path descriptors first, command descriptors second.
- Plain gRPC and TLS gRPC.
- Static headers and bearer token auth.
- Primitive Arrow types, strings, binaries, dates, timestamps, decimals where
  straightforward, and dictionary decoding if it is already natural in the
  Arrow path.
- Nested types marked visible but unsupported until type mapping is proven.
- Projection-aware materialization in DuckDB.
- Streaming scan with bounded buffering.
- Single endpoint scan first, then sequential multi-endpoint scan.

Explicitly defer:

- Flight SQL.
- Writes through `DoPut`.
- Actions beyond metadata discovery.
- Parallel endpoint scanning.
- Complex auth flows.
- Nested data flattening.
- Custom service-specific pushdown protocols.
- `PollFlightInfo` for long-running queries.

## Performance Plan

Performance work should be built into the design, not saved as polish:

- Bind once, stream many batches.
- Reuse client channels only after correctness and cancellation are proven.
- Avoid buffering full Flight results.
- Materialize only requested columns.
- Prefer vectorized Arrow-to-DuckDB conversion paths.
- Track copy count per type family during benchmarks.
- Measure latency to first row separately from total throughput.
- Add local benchmark fixtures with controllable row counts, column counts,
  dictionary encoding, batch sizes, and endpoint counts.
- Treat parallel endpoint reads as a later feature with explicit backpressure.

Benchmark scenarios:

- local loopback Flight server
- TLS local server
- many narrow columns
- few wide columns
- small batches
- large batches
- multi-endpoint dataset
- high-cardinality strings
- dictionary-encoded columns
- projected subset of a wide schema
- scan with unsupported columns excluded
- endpoint fanout with ordered and unordered results
- simulated WAN latency and transient `UNAVAILABLE` failures

Lakehouse scenarios:

- governed dataset service with bearer auth
- Flight SQL service over a table backed by object storage
- endpoint-per-partition stream that models partitioned lake data
- large curated result joined locally with Parquet or Iceberg data in DuckDB

## Pushdown Plan

Pushdown should be explicit by capability:

- Phase 1: projection-aware materialization in the DuckDB bridge.
- Phase 2: projection hints through Flight descriptors or application metadata
  only for services that document support.
- Phase 3: Flight SQL query pushdown for services that implement Flight SQL.
- Phase 4: conservative filter pushdown for simple comparisons when the target
  service advertises support or the function form makes it explicit.
- Phase 5: service-specific pushdown profiles only when they are named,
  documented, and disabled by default.

Avoid silently changing query semantics. If a filter cannot be pushed down, scan
and filter in DuckDB. If a service claims pushdown and returns inconsistent
schema or metadata, fail with an error that names the unsupported contract.

## Testing Plan

Test layers:

- Rust unit tests for descriptor, ticket, options, auth metadata, and type
  mapping.
- Rust integration tests against an in-process Flight server.
- SQLLogic tests for DuckDB-visible behavior.
- Error tests for auth failure, TLS failure, missing descriptor, schema mismatch,
  stream interruption, unsupported type, and deadline exceeded.
- Protocol tests for ordered endpoints, unordered endpoints, expired endpoints,
  empty endpoint locations, reuse-connection locations, and binary tickets.
- Golden tests for metadata table output.
- Cancellation and backpressure tests for long streams.
- Benchmark tests kept separate from correctness tests.

Fixtures should include:

- simple primitive schema
- nullable fields
- timestamps and time zones
- strings and binaries
- decimal values
- dictionary data
- nested data marked expected-fail until supported
- multi-batch stream
- multi-endpoint flight
- delayed stream for cancellation
- auth-required service
- Flight SQL service fixture once Phase 6 starts
- lakehouse-shaped fixture with partitioned endpoints and realistic metadata

## Delivery Plan

Phase 0 acceptance checklist:

- [x] The team can state whether the first implementation is Rust-only or C++ shim
  plus Rust core.
- [x] A tiny local Flight client can fetch schema and stream batches through the
  chosen async runtime shape.
- [x] Arrow-to-DuckDB type mapping has a documented supported subset and expected
  failures.
- [x] The first public SQL surface is confirmed, with `flight(...)` included as the
  friendly v0.1 entrypoint.
- [x] Packaging has been smoke-tested for at least local unsigned loading on one
  developer machine.

Phase 0: design and spike

- [x] Finalize this design doc.
- [x] Create `SPEC.md` with SQL examples and result schemas.
- Create `ARCHITECTURE.md` with component boundaries.
- Create `ROADMAP.md` with checkboxes.
- [x] Validate Rust template support for required DuckDB APIs.
- [x] Prototype type mapping inside the extension against a real Arrow fixture.
- [x] Prototype async `DoGet` consumption against a tiny Flight service.
- [x] Write a one-page "Flight vs native lakehouse extension" positioning note.
- [x] Make a go/no-go decision on Rust-only vs C++ shim.

Phase 1: extension skeleton

- [x] Scaffold from the DuckDB Rust extension template.
- [x] Register first real metadata functions.
- [x] Add SQLLogic smoke tests.
- [x] Add local Flight test server fixture.
- [ ] Establish CI and packaging beyond local unsigned builds.
- [x] Publish developer setup instructions.

Phase 2: metadata and schema

- [x] Implement `flight_list`.
- [x] Implement `flight_info`.
- [x] Implement `flight_schema`.
- [x] Implement `flight_check`.
- [x] Map Arrow schema to DuckDB logical types.
- [ ] Normalize gRPC, TLS, Arrow, and auth errors into a polished diagnostic
  taxonomy.

Phase 3: streaming scans

- [x] Implement `flight_scan` for a single endpoint.
- [x] Add `flight` as the friendly alias.
- [x] Implement `flight_scan_ticket` if ticket handling is stable.
- [x] Convert Arrow batches to DuckDB chunks for the supported type subset.
- [x] Replace descriptor-scan bind-time batch collection with execution-time
  endpoint streaming.
- [x] Add an explicit-schema mode for ticket-only scans so they can avoid
  bind-time prebuffering.
- [x] Add sequential multi-endpoint scanning.
- [x] Preserve opaque endpoint tickets and support `hex`/`base64` explicit
  ticket scans.
- [ ] Add bounded producer/consumer buffering for background async reads.
- [ ] Add cancellation behavior.
- [ ] Add projection-aware materialization.

Phase 4: ergonomics and auth

- [x] Add named options or stable structured options.
- [ ] Add TLS configuration beyond `grpc+tls://` and native roots.
- [x] Add bearer token and headers.
- [x] Add `timeout_ms` for connect/RPC timeout control.
- [ ] Improve error messages.
- [x] Add first-five-minutes README examples.
- [x] Add docs for choosing `flight(...)` vs `read_parquet`, `iceberg`, `delta`,
  `lance`, or `ducklake`.
- [x] Add a README voice pass that keeps the project approachable without hiding
  planning status.

Phase 5: performance and release candidate

- [ ] Add benchmark harness.
- [ ] Measure copy counts and throughput.
- [x] Add sequential multi-endpoint scanning.
- [ ] Add ordered/unordered endpoint behavior tests.
- [ ] Decide whether connection reuse is needed for v0.1.
- [ ] Add backpressure and cancellation tests.
- [ ] Add `make demo`, `make test`, and `make bench` targets or documented
  equivalents.
- [ ] Publish benchmark numbers.

Phase 6: Flight SQL and advanced pushdown

- Add `flight_sql`.
- Push down SQL through Flight SQL services.
- Add simple filter pushdown where capability is explicit.
- Explore prepared statements and parameter binding.
- Add `PollFlightInfo` support if real services need long-running query polling.

Phase 7: distribution readiness

- [x] Rename local build and extension identity to `flight`.
- [x] Pin the DuckDB Rust crate exactly to the target DuckDB patch version.
- [x] Add community extension descriptor template.
- [x] Add release readiness checker for identity/version/descriptor drift.
- [x] Add community descriptor renderer for immutable release refs.
- [x] Add GitHub workflow to render the community descriptor artifact.
- [x] Add Rust quality workflow for fmt, test, clippy, and metadata checks.
- [x] Add license file required for open-source release.
- [x] Update CI workflow extension name to `flight`.
- [ ] Harden platform packaging in community CI.
- [ ] Document examples against real Flight services.
- [x] Document modern lakehouse examples and anti-examples.
- [ ] Add extension install smoke tests against a signed community artifact.
- [ ] Submit `extensions/flight/description.yml` to `duckdb/community-extensions`
  after the repository URL and release commit are final.

## CTO Delivery View

Suggested initial staffing:

- One DuckDB extension engineer for table functions, packaging, and SQLLogic.
- One Rust/Arrow engineer for Flight client, async runtime, and type conversion.
- Shared review from a data platform user to keep the SQL surface honest.

Suggested release gates:

- Gate 0: `SPEC`, release-readiness scripts, and Rust-template spike are done.
- Gate 1: local extension loads and metadata functions return stable schemas.
- Gate 2: single-endpoint streaming scan works with bounded memory and
  cancellation.
- Gate 3: auth, TLS, error handling, and projection behavior are documented and
  tested.
- Gate 4: benchmark results and platform packages are good enough for v0.1.
- Gate 5: README explains where Flight fits beside native DuckDB lakehouse
  extensions.
- Gate 6: local demo, test, and benchmark workflows are documented and run in a
  clean checkout.

Primary risks:

- Rust template cannot expose needed DuckDB hooks cleanly.
- Async Flight streams do not fit neatly into DuckDB scan lifecycle.
- Arrow-to-DuckDB conversion copies too much data for the performance promise.
- Auth/TLS packaging causes cross-platform build friction.
- Flight services vary widely in descriptor, ticket, metadata, and pushdown
  behavior.
- Users may expect this to replace lakehouse table-format extensions unless the
  positioning is explicit.

Mitigations:

- Retire extension API risk in Phase 0 before feature work.
- Keep a C++ shim fallback explicit.
- Use an in-process Flight server fixture from the start.
- Publish unsupported-type behavior as a feature, not an accident.
- Keep Flight SQL out of the critical path until read scans are proven.
- Keep "use native DuckDB scans when you can, use Flight when you need a service
  boundary" in the README and examples.
- Keep the README honest: fun is welcome, but status, limitations, and next
  steps must stay obvious.

## Open Questions

- Should `flight(...)` accept only path descriptors, or also infer command
  descriptors from strings?
- How should path descriptors be represented: DuckDB `LIST<VARCHAR>`, slash
  string, or both?
- Is ticket text always acceptable, or do we need binary ticket input from day
  one?
- Should `flight_check` be part of v0.1, or only a development helper?
- Which Arrow nested types should be supported in v0.1?
- Is a JSON options argument acceptable temporarily, or should named options be a
  release blocker?
- Can the Rust template support the needed table-function hooks directly?
- What should the public story be for DuckDB secrets integration?
- Should the extension depend on Arrow C Data Interface for lower-copy transfer?
- How should service capability discovery be represented in SQL?
- Which TLS root strategy gives the best balance of local ergonomics and
  reproducible releases?
- Which real Flight or Flight SQL services should be in the compatibility matrix
  before a public release?
- Should `make demo` launch DuckDB directly, print SQL to paste, or run a guided
  SQL script end to end?

## References

- DuckDB Rust extension template:
  <https://github.com/duckdb/extension-template-rs>
- DuckDB C table function API:
  <https://duckdb.org/docs/stable/clients/c/table_functions.html>
- Arrow Flight overview:
  <https://arrow.apache.org/docs/format/Flight.html>
- Arrow Flight SQL:
  <https://arrow.apache.org/docs/format/FlightSql.html>
- ADBC Flight SQL driver:
  <https://arrow.apache.org/adbc/main/driver/flight_sql.html>
- DuckDB lakehouse formats:
  <https://duckdb.org/docs/current/lakehouse_formats>
- Rust `arrow-flight` crate:
  <https://docs.rs/arrow-flight/latest/arrow_flight/>
