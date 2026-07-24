# flight SQL Specification

This document is the release-facing contract for the DuckDB `flight` extension.
It should stay close to the implementation and should be updated before any
community-extension release changes the public SQL surface.

## Extension Identity

```sql
INSTALL flight FROM community;
LOAD flight;
```

Local development builds produce `flight.duckdb_extension`.

## Locations

`location` identifies an Arrow Flight service or an offline fixture:

- `grpc://host:port` maps to a plaintext Flight endpoint.
- `grpc+tls://host:port` maps to a TLS Flight endpoint using native roots.
- `fixture://events` exposes a tiny synthetic event stream.
- `fixture://seattle-weather` exposes a tiny public Seattle weather sample.
- `fixture://palmer-penguins` exposes a tiny public non-human biology sample.

## Descriptors

Descriptor scans use Arrow Flight descriptors:

- `descriptor_type := 'path'` with `descriptor := ['warehouse', 'events']`.
- `descriptor_type := 'cmd'` or `descriptor_type := 'command'` with
  `descriptor := 'select * from events'`.

The friendly `flight(location, descriptor, ...)` function defaults to a path
descriptor.

## Shared Options

The first release supports a small named-option set:

- `bearer_token := 'token'` sends `authorization: Bearer token`.
- `authorization_header := 'Basic ...'` sends an explicit authorization value.
- `max_message_size := 134217728` sets the gRPC receive limit in bytes.
- `timeout_ms := 30000` sets connect and RPC timeouts in milliseconds.

`authorization_header` wins when both auth options are supplied.

Planned options include custom TLS roots, hostname override, DuckDB secrets
integration, and development-only TLS verification controls.

## Table Functions

### `flight_check`

```sql
SELECT * FROM flight_check(location, ...);
```

Returns diagnostic rows with stable columns:

| column | type | meaning |
| --- | --- | --- |
| `check_name` | `VARCHAR` | Diagnostic category. |
| `status` | `VARCHAR` | `ok`, `warning`, or `error`. |
| `message` | `VARCHAR` | Human-readable result. |
| `detail` | `VARCHAR` | Service, fixture, or error detail. |

### `flight_list`

```sql
SELECT * FROM flight_list(location, ...);
```

Returns one row per advertised flight:

| column | type | meaning |
| --- | --- | --- |
| `descriptor_type` | `VARCHAR` | `path`, `cmd`, or `unknown`. |
| `descriptor` | `VARCHAR` | Display form of the descriptor. |
| `endpoint_count` | `BIGINT` | Number of endpoints in `FlightInfo`. |
| `total_records` | `BIGINT` | Server record count hint, or `-1`. |
| `total_bytes` | `BIGINT` | Server byte count hint, or `-1`. |
| `ordered` | `BOOLEAN` | Server ordered-endpoints flag. |
| `app_metadata` | `VARCHAR` | UTF-8 metadata display, when present. |

### `flight_info`

```sql
SELECT *
FROM flight_info(
  location,
  descriptor_type := 'path',
  descriptor := ['warehouse', 'events']
);
```

Returns one row per endpoint:

| column | type | meaning |
| --- | --- | --- |
| `endpoint_index` | `BIGINT` | Endpoint order from the service. |
| `location` | `VARCHAR` | Endpoint location, or the service location. |
| `ticket` | `VARCHAR` | UTF-8 ticket display, or `hex:<value>` for non-text tickets. |
| `ticket_hex` | `VARCHAR` | Exact lowercase hex encoding of the opaque ticket bytes. |
| `ticket_size` | `BIGINT` | Ticket size in bytes. |
| `expiration_time` | `VARCHAR` | Endpoint expiration when supplied. |
| `total_records` | `BIGINT` | Flight-level record count hint, or `-1`. |
| `total_bytes` | `BIGINT` | Flight-level byte count hint, or `-1`. |
| `app_metadata` | `VARCHAR` | Endpoint metadata display, when present. |

### `flight_schema`

```sql
SELECT *
FROM flight_schema(location, ['warehouse', 'events']);
```

Returns one row per Arrow field:

| column | type | meaning |
| --- | --- | --- |
| `field_index` | `BIGINT` | Field position. |
| `field_name` | `VARCHAR` | Arrow field name. |
| `arrow_type` | `VARCHAR` | Arrow datatype display. |
| `duckdb_type` | `VARCHAR` | Planned DuckDB logical type display. |
| `nullable` | `BOOLEAN` | Arrow nullability flag. |
| `metadata` | `VARCHAR` | Field metadata display. |
| `supported` | `BOOLEAN` | Whether the current bridge expects to scan it. |

### `flight` and `flight_scan`

```sql
SELECT *
FROM flight(location, ['warehouse', 'events']);

SELECT *
FROM flight_scan(
  location,
  descriptor_type := 'path',
  descriptor := ['warehouse', 'events']
);
```

`flight` is a friendly alias over `flight_scan` for path descriptors. Both bind
the result schema from `FlightInfo`, then stream endpoint tickets during
execution. Multi-endpoint scans are currently consumed sequentially.

### `flight_scan_ticket`

```sql
SELECT *
FROM flight_scan_ticket(
  location,
  ticket := 'opaque-ticket',
  schema_descriptor := ['warehouse', 'events']
);

SELECT *
FROM flight_scan_ticket(
  location,
  ticket := '00ff61',
  ticket_encoding := 'hex',
  schema_descriptor := ['warehouse', 'events']
);
```

Ticket scans stream when `schema_descriptor` is supplied, because DuckDB needs a
bind-time schema. A bare ticket can still scan, but it prebuffers the stream to
discover schema and should not be used for large results.

`ticket_encoding` defaults to `utf8`. Use `hex` or `base64` when a service
returns opaque/non-text tickets. `flight_info.ticket_hex` is the safest value to
copy into `flight_scan_ticket(..., ticket_encoding := 'hex')`.

## Current Type Contract

The implementation relies on the DuckDB Rust/Arrow bridge for conversion. The
release test suite currently exercises booleans, signed integers, floats,
UTF-8 strings, dates, timestamps, nulls, and grouped analytical queries over
real Arrow Flight streams. The protocol tests also cover binary endpoint
tickets, because Arrow Flight tickets are opaque bytes rather than strings.

Before a broader v0.1 release, the type-support table should be expanded with
explicit pass/fail coverage for unsigned integers, decimals, binary values,
lists, structs, dictionaries, and timestamps with time zones.

## Reliability Contract

The extension should fail with actionable DuckDB errors for:

- Invalid locations or unsupported URI schemes.
- Missing descriptors or tickets.
- Unreachable services.
- Authentication or TLS failures.
- Unsupported Arrow fields selected by a scan.
- Interrupted streams.

The first release should not claim Flight SQL, writes, broad pushdown, global
connection pooling, or zero-copy behavior until those features are implemented
and tested.
