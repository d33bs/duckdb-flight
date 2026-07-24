# Architecture

`flight` is a DuckDB table-function extension backed by a Rust Arrow Flight
client.

## Layers

- **SQL layer** registers DuckDB table functions: `flight`, `flight_scan`,
  `flight_scan_ticket`, `flight_check`, `flight_list`, `flight_info`, and
  `flight_schema`.
- **Bind layer** parses locations, descriptors, tickets, and named options. It
  binds DuckDB result columns from Arrow schemas before execution starts.
- **Flight client layer** handles gRPC channels, TLS URI mapping, auth metadata,
  message-size limits, timeouts, and Arrow Flight RPCs.
- **Stream layer** converts `FlightData` streams into Arrow `RecordBatch` values.
- **DuckDB bridge** converts Arrow batches into DuckDB data chunks.
- **Fixture layer** provides deterministic local data sources for tests and
  examples.

## Scan Flow

Descriptor scans:

1. DuckDB calls the bind function.
2. The extension calls `GetFlightInfo`.
3. The Arrow schema in `FlightInfo` becomes the DuckDB output schema.
4. Execution opens each endpoint ticket with `DoGet`.
5. Record batches are streamed into DuckDB chunks.

Ticket scans:

1. DuckDB calls the bind function.
2. If `schema_descriptor` is provided, the extension calls `GetSchema` and
   streams the supplied ticket during execution.
3. Without `schema_descriptor`, the extension prebuffers the ticket stream during
   bind to discover the schema.

## Locations

- `grpc://host:port` maps to a plaintext Flight endpoint.
- `grpc+tls://host:port` maps to a TLS Flight endpoint using native roots.
- `fixture://events`, `fixture://seattle-weather`, and
  `fixture://palmer-penguins` use local Arrow batches.

## Tickets

Arrow Flight tickets are opaque bytes. `flight_info` exposes:

- `ticket`: UTF-8 display when possible, otherwise `hex:<value>`.
- `ticket_hex`: exact lowercase hex encoding.
- `ticket_size`: ticket length in bytes.

`flight_scan_ticket` accepts `ticket_encoding := 'utf8' | 'hex' | 'base64'`.

## Options

- `bearer_token`: sends `authorization: Bearer <token>`.
- `authorization_header`: sends an explicit authorization value.
- `max_message_size`: sets gRPC receive and send limits in bytes.
- `timeout_ms`: sets connect and RPC timeouts in milliseconds.

`authorization_header` takes precedence over `bearer_token`.

## Tests

- Rust tests cover descriptor parsing, fixture batches, ticket encodings, and a
  loopback Arrow Flight server serving real `FlightData` streams.
- SQLLogic tests cover loading, metadata functions, schemas, scans, real-data
  fixtures, ticket scans, and ticket error paths.
- `prek` runs `rustfmt` and Clippy through `.pre-commit-config.yaml`.
