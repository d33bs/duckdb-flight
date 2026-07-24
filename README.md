<img src="docs/assets/duckdb-flight-logo.png" alt="duckdb-flight logo" width="160">

# duckdb-flight

Query Arrow Flight services from DuckDB with fast, ergonomic SQL.

Stop waddling files around. Let Flight carry the batches.

`flight` is a DuckDB extension for reading Arrow Flight streams with ordinary
SQL. It keeps DuckDB convenient for local analysis while a remote service
delivers governed, high-throughput Arrow batches.

Descriptor scans bind their schema from `FlightInfo` and stream endpoint tickets
during execution. Ticket scans also stream when a `schema_descriptor` is
provided. Bare ticket scans prebuffer the stream because a bare ticket does not
expose a bind-time schema to DuckDB.

## Install

```sql
INSTALL flight FROM community;
LOAD flight;
```

For local development builds:

```sql
LOAD './build/debug/flight.duckdb_extension';
```

## First Five Minutes

Use built-in fixtures to try the extension without running a Flight service:

```sql
SELECT * FROM flight_check('fixture://events');
SELECT * FROM flight_list('fixture://events');
SELECT * FROM flight_schema('fixture://events', ['warehouse', 'events']);
SELECT * FROM flight('fixture://events', ['warehouse', 'events']);

-- Public Seattle weather sample.
SELECT weather, round(sum(precipitation), 1) AS precipitation
FROM flight('fixture://seattle-weather', ['public', 'seattle_weather'])
GROUP BY weather
ORDER BY precipitation DESC;

-- Public non-human biology sample from Palmer penguins.
SELECT species, count(*) AS penguins, round(avg(body_mass_g), 1) AS avg_body_mass_g
FROM flight('fixture://palmer-penguins', ['bio', 'palmer_penguins'])
GROUP BY species
ORDER BY species;
```

Against a Flight service:

```sql
-- Is the service reachable?
SELECT * FROM flight_check('grpc://localhost:5005');

-- What can I query?
SELECT * FROM flight_list('grpc://localhost:5005');

-- What shape is this dataset?
SELECT *
FROM flight_schema('grpc://localhost:5005', ['warehouse', 'events']);

-- Bring a stream into DuckDB.
CREATE VIEW events AS
SELECT *
FROM flight('grpc://localhost:5005', ['warehouse', 'events']);

SELECT event_type, count(*)
FROM events
GROUP BY event_type
ORDER BY count(*) DESC;
```

With bearer-token auth and request timeout:

```sql
SELECT *
FROM flight(
  'grpc+tls://flight.example.com',
  ['warehouse', 'events'],
  bearer_token := getenv('FLIGHT_TOKEN'),
  timeout_ms := 30000
)
LIMIT 100;
```

## When To Use It

Use `flight` when the data boundary is a service:

- Governed data products.
- Remote query engines returning Arrow streams.
- Private-network or cross-cloud datasets.
- Results partitioned across Flight endpoints.
- Curated slices joined locally with Parquet, Iceberg, Delta, Lance, DuckLake,
  CSV, or in-memory data.

Use native DuckDB scans when the files or table format are the product:

- `read_parquet(...)` for direct Parquet access.
- `iceberg`, `delta`, `lance`, or `ducklake` for native lakehouse tables.
- DuckDB filesystem and secrets support when DuckDB owns the I/O.

Short version: use native DuckDB scans when you can; use Flight when you need a
service boundary.

## SQL Surface

```sql
SELECT * FROM flight_check('grpc://localhost:5005');
SELECT * FROM flight_list('grpc://localhost:5005');
SELECT * FROM flight_schema('grpc://localhost:5005', ['warehouse', 'events']);
SELECT * FROM flight('grpc://localhost:5005', ['warehouse', 'events']);
```

Lower-level functions expose explicit descriptor and ticket scans:

```sql
SELECT *
FROM flight_scan(
  'grpc://localhost:5005',
  descriptor_type := 'path',
  descriptor := ['warehouse', 'events']
);

SELECT *
FROM flight_scan_ticket(
  'grpc://localhost:5005',
  ticket := '...',
  schema_descriptor := ['warehouse', 'events']
);
```

Flight tickets are opaque bytes. `flight_info` exposes both a display value and
an exact hex value:

```sql
SELECT endpoint_index, location, ticket, ticket_hex, ticket_size
FROM flight_info('grpc://localhost:5005', ['warehouse', 'events']);

SELECT *
FROM flight_scan_ticket(
  'grpc://localhost:5005',
  ticket := '00ff61',
  ticket_encoding := 'hex',
  schema_descriptor := ['warehouse', 'events']
);
```

## Development

```sh
make configure debug
make test_rust
make test_debug
make lint
```

Useful local data sources:

- `fixture://events`
- `fixture://seattle-weather`
- `fixture://palmer-penguins`

`cargo test` runs Rust unit tests with an in-process Arrow Flight server that
emits real `FlightData` streams over gRPC.

## Documentation

- [SQL Specification](SPEC.md)
- [Architecture](docs/architecture.md)
- [Community Extension Release](docs/release/community-extension.md)
- [Contributing](CONTRIBUTING.md)
