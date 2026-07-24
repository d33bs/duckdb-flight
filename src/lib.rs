use std::{
    error::Error,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use arrow_array::{
    ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow_flight::{
    decode::FlightRecordBatchStream, flight_service_client::FlightServiceClient, Criteria,
    FlightDescriptor, FlightEndpoint, FlightInfo, SchemaResult, Ticket,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{
        arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type},
        BindInfo, InitInfo, TableFunctionInfo, VTab, Value,
    },
    Connection, Result,
};
use futures::{Stream, StreamExt, TryStreamExt};
use tokio::runtime::Runtime;
use tonic::{
    metadata::{AsciiMetadataValue, MetadataKey},
    transport::{Channel, Endpoint},
    Request,
};

const EVENTS_FIXTURE_LOCATION: &str = "fixture://events";
const WEATHER_FIXTURE_LOCATION: &str = "fixture://seattle-weather";
const PENGUINS_FIXTURE_LOCATION: &str = "fixture://palmer-penguins";
const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
struct FlightOptions {
    bearer_token: Option<String>,
    authorization_header: Option<String>,
    max_message_size: usize,
    timeout: Option<Duration>,
}

impl Default for FlightOptions {
    fn default() -> Self {
        Self {
            bearer_token: None,
            authorization_header: None,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            timeout: None,
        }
    }
}

#[derive(Debug, Clone)]
enum Descriptor {
    Path(Vec<String>),
    Command(String),
}

impl Descriptor {
    fn to_flight_descriptor(&self) -> FlightDescriptor {
        match self {
            Self::Path(path) => FlightDescriptor::new_path(path.clone()),
            Self::Command(command) => FlightDescriptor::new_cmd(command.clone()),
        }
    }
}

#[derive(Debug, Clone)]
enum Cell {
    String(String),
    I64(i64),
    Bool(bool),
}

#[derive(Debug, Clone, Copy)]
enum ColumnKind {
    String,
    I64,
    Bool,
}

#[derive(Debug, Clone, Copy)]
struct ColumnSpec {
    name: &'static str,
    kind: ColumnKind,
}

impl ColumnSpec {
    fn logical_type(&self) -> LogicalTypeHandle {
        match self.kind {
            ColumnKind::String => LogicalTypeHandle::from(LogicalTypeId::Varchar),
            ColumnKind::I64 => LogicalTypeHandle::from(LogicalTypeId::Bigint),
            ColumnKind::Bool => LogicalTypeHandle::from(LogicalTypeId::Boolean),
        }
    }
}

struct RowsBindData {
    rows: Vec<Vec<Cell>>,
}

struct RowsInitData {
    offset: AtomicUsize,
}

fn parse_options(bind: &BindInfo) -> Result<FlightOptions, Box<dyn Error>> {
    let mut options = FlightOptions::default();
    if let Some(value) = bind.get_named_parameter("bearer_token") {
        if !value.is_null() {
            options.bearer_token = Some(value.to_string());
        }
    }
    if let Some(value) = bind.get_named_parameter("authorization_header") {
        if !value.is_null() {
            options.authorization_header = Some(value.to_string());
        }
    }
    if let Some(value) = bind.get_named_parameter("max_message_size") {
        if !value.is_null() {
            options.max_message_size = usize::try_from(value.to_uint64())
                .map_err(|_| "max_message_size does not fit in usize")?;
            if options.max_message_size == 0 {
                return Err("max_message_size must be greater than 0".into());
            }
        }
    }
    if let Some(value) = bind.get_named_parameter("timeout_ms") {
        if !value.is_null() {
            let timeout_ms = value.to_uint64();
            if timeout_ms == 0 {
                return Err("timeout_ms must be greater than 0".into());
            }
            options.timeout = Some(Duration::from_millis(timeout_ms));
        }
    }
    Ok(options)
}

fn runtime() -> Result<Runtime, Box<dyn Error>> {
    Runtime::new().map_err(|err| format!("failed to create Tokio runtime: {err}").into())
}

async fn connect(
    location: &str,
    options: &FlightOptions,
) -> Result<FlightServiceClient<Channel>, Box<dyn Error>> {
    let mut endpoint = Endpoint::from_shared(tonic_location(location))
        .map_err(|err| format!("invalid Flight location {location}: {err}"))?;
    if let Some(timeout) = options.timeout {
        endpoint = endpoint.connect_timeout(timeout).timeout(timeout);
    }
    let channel = endpoint
        .connect()
        .await
        .map_err(|err| format!("failed to connect to Flight service {location}: {err}"))?;
    let client = FlightServiceClient::new(channel)
        .max_decoding_message_size(options.max_message_size)
        .max_encoding_message_size(options.max_message_size);
    Ok(client)
}

fn tonic_location(location: &str) -> String {
    if let Some(rest) = location.strip_prefix("grpc://") {
        format!("http://{rest}")
    } else if let Some(rest) = location.strip_prefix("grpc+tls://") {
        format!("https://{rest}")
    } else {
        location.to_string()
    }
}

fn with_auth<T>(
    mut request: Request<T>,
    options: &FlightOptions,
) -> Result<Request<T>, Box<dyn Error>> {
    let header = options.authorization_header.clone().or_else(|| {
        options
            .bearer_token
            .as_ref()
            .map(|token| format!("Bearer {token}"))
    });
    if let Some(header) = header {
        let key = MetadataKey::from_static("authorization");
        let value = AsciiMetadataValue::try_from(header.as_str())
            .map_err(|err| format!("invalid authorization header: {err}"))?;
        request.metadata_mut().insert(key, value);
    }
    Ok(request)
}

fn descriptor_from_bind(bind: &BindInfo, default_kind: &str) -> Result<Descriptor, Box<dyn Error>> {
    let descriptor_value = bind.get_parameter(1);
    let descriptor_type = bind
        .get_named_parameter("descriptor_type")
        .map(|value| value.to_string())
        .unwrap_or_else(|| default_kind.to_string());
    descriptor_from_value(&descriptor_value, &descriptor_type)
}

fn descriptor_from_value(
    descriptor_value: &Value,
    descriptor_type: &str,
) -> Result<Descriptor, Box<dyn Error>> {
    if descriptor_type.eq_ignore_ascii_case("path") {
        if let Some(parts) = descriptor_value.to_list() {
            let path = parts
                .iter()
                .filter(|value| !value.is_null())
                .map(ToString::to_string)
                .collect();
            return Ok(Descriptor::Path(path));
        }
    }
    parse_descriptor(descriptor_type, &descriptor_value.to_string())
}

fn schema_descriptor_from_bind(bind: &BindInfo) -> Result<Option<Descriptor>, Box<dyn Error>> {
    let Some(descriptor_value) = bind.get_named_parameter("schema_descriptor") else {
        return Ok(None);
    };
    if descriptor_value.is_null() {
        return Ok(None);
    }
    let descriptor_type = bind
        .get_named_parameter("schema_descriptor_type")
        .map(|value| value.to_string())
        .unwrap_or_else(|| "path".to_string());
    descriptor_from_value(&descriptor_value, &descriptor_type).map(Some)
}

fn parse_descriptor(
    descriptor_type: &str,
    descriptor_value: &str,
) -> Result<Descriptor, Box<dyn Error>> {
    match descriptor_type.to_ascii_lowercase().as_str() {
        "path" => Ok(Descriptor::Path(parse_path_descriptor(descriptor_value))),
        "cmd" | "command" => Ok(Descriptor::Command(descriptor_value.to_string())),
        other => Err(format!("descriptor_type must be path or cmd, got {other}").into()),
    }
}

fn parse_path_descriptor(value: &str) -> Vec<String> {
    value
        .split('/')
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

#[derive(Debug, Clone, Copy)]
enum FixtureKind {
    Events,
    SeattleWeather,
    PalmerPenguins,
}

fn fixture_kind(location: &str) -> Option<FixtureKind> {
    match location {
        EVENTS_FIXTURE_LOCATION => Some(FixtureKind::Events),
        WEATHER_FIXTURE_LOCATION => Some(FixtureKind::SeattleWeather),
        PENGUINS_FIXTURE_LOCATION => Some(FixtureKind::PalmerPenguins),
        _ => None,
    }
}

fn fixture_descriptor(kind: FixtureKind) -> Vec<String> {
    match kind {
        FixtureKind::Events => vec!["warehouse".to_string(), "events".to_string()],
        FixtureKind::SeattleWeather => vec!["public".to_string(), "seattle_weather".to_string()],
        FixtureKind::PalmerPenguins => vec!["bio".to_string(), "palmer_penguins".to_string()],
    }
}

fn fixture_ticket(kind: FixtureKind) -> &'static str {
    match kind {
        FixtureKind::Events => "fixture-events",
        FixtureKind::SeattleWeather => "fixture-seattle-weather",
        FixtureKind::PalmerPenguins => "fixture-palmer-penguins",
    }
}

fn fixture_ticket_bytes(kind: FixtureKind) -> Vec<u8> {
    fixture_ticket(kind).as_bytes().to_vec()
}

fn validate_fixture_ticket(kind: FixtureKind, ticket: &[u8]) -> Result<(), Box<dyn Error>> {
    let expected = fixture_ticket_bytes(kind);
    if ticket == expected {
        Ok(())
    } else {
        Err(format!(
            "unknown fixture ticket {}; expected {}",
            bytes_display(ticket),
            fixture_ticket(kind)
        )
        .into())
    }
}

fn fixture_schema(kind: FixtureKind) -> SchemaRef {
    match kind {
        FixtureKind::Events => Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, false),
            Field::new("event_type", DataType::Utf8, false),
            Field::new("amount", DataType::Float64, true),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ])),
        FixtureKind::SeattleWeather => Arc::new(Schema::new(vec![
            Field::new("date", DataType::Date32, false),
            Field::new("precipitation", DataType::Float64, false),
            Field::new("temp_max", DataType::Float64, false),
            Field::new("temp_min", DataType::Float64, false),
            Field::new("wind", DataType::Float64, false),
            Field::new("weather", DataType::Utf8, false),
        ])),
        FixtureKind::PalmerPenguins => Arc::new(Schema::new(vec![
            Field::new("species", DataType::Utf8, false),
            Field::new("island", DataType::Utf8, false),
            Field::new("bill_length_mm", DataType::Float64, true),
            Field::new("bill_depth_mm", DataType::Float64, true),
            Field::new("flipper_length_mm", DataType::Int64, true),
            Field::new("body_mass_g", DataType::Int64, true),
            Field::new("sex", DataType::Utf8, true),
            Field::new("year", DataType::Int64, false),
        ])),
    }
}

fn fixture_batches(kind: FixtureKind) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let schema = fixture_schema(kind);
    let batch = match kind {
        FixtureKind::Events => RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "signup", "purchase", "purchase", "refund",
                ])) as ArrayRef,
                Arc::new(Float64Array::from(vec![
                    None,
                    Some(42.5),
                    Some(9.99),
                    Some(-9.99),
                ])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1_704_067_200_000_000,
                    1_704_070_800_000_000,
                    1_704_074_400_000_000,
                    1_704_078_000_000_000,
                ])) as ArrayRef,
            ],
        )?,
        FixtureKind::SeattleWeather => RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![
                    15340, 15341, 15342, 15343, 15344, 15347,
                ])) as ArrayRef,
                Arc::new(Float64Array::from(vec![0.0, 10.9, 0.8, 20.3, 1.3, 0.0])) as ArrayRef,
                Arc::new(Float64Array::from(vec![12.8, 10.6, 11.7, 12.2, 8.9, 10.0])) as ArrayRef,
                Arc::new(Float64Array::from(vec![5.0, 2.8, 7.2, 5.6, 2.8, 2.8])) as ArrayRef,
                Arc::new(Float64Array::from(vec![4.7, 4.5, 2.3, 4.7, 6.1, 2.0])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "drizzle", "rain", "rain", "rain", "rain", "sun",
                ])) as ArrayRef,
            ],
        )?,
        FixtureKind::PalmerPenguins => RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![
                    "Adelie",
                    "Adelie",
                    "Adelie",
                    "Gentoo",
                    "Gentoo",
                    "Gentoo",
                    "Chinstrap",
                    "Chinstrap",
                    "Chinstrap",
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "Torgersen",
                    "Torgersen",
                    "Torgersen",
                    "Biscoe",
                    "Biscoe",
                    "Biscoe",
                    "Dream",
                    "Dream",
                    "Dream",
                ])) as ArrayRef,
                Arc::new(Float64Array::from(vec![
                    Some(39.1),
                    Some(39.5),
                    Some(40.3),
                    Some(46.1),
                    Some(50.0),
                    Some(48.7),
                    Some(46.5),
                    Some(50.0),
                    Some(51.3),
                ])) as ArrayRef,
                Arc::new(Float64Array::from(vec![
                    Some(18.7),
                    Some(17.4),
                    Some(18.0),
                    Some(13.2),
                    Some(16.3),
                    Some(14.1),
                    Some(17.9),
                    Some(19.5),
                    Some(19.2),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(181),
                    Some(186),
                    Some(195),
                    Some(211),
                    Some(230),
                    Some(210),
                    Some(192),
                    Some(196),
                    Some(193),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    Some(3750),
                    Some(3800),
                    Some(3250),
                    Some(4500),
                    Some(5700),
                    Some(4450),
                    Some(3500),
                    Some(3900),
                    Some(3650),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("male"),
                    Some("female"),
                    Some("female"),
                    Some("female"),
                    Some("male"),
                    Some("female"),
                    Some("female"),
                    Some("male"),
                    Some("male"),
                ])) as ArrayRef,
                Arc::new(Int64Array::from(vec![
                    2007, 2007, 2007, 2007, 2007, 2007, 2007, 2007, 2007,
                ])) as ArrayRef,
            ],
        )?,
    };
    Ok(vec![batch])
}

fn fixture_flight_info(kind: FixtureKind) -> Result<FlightInfo, Box<dyn Error>> {
    let batches = fixture_batches(kind)?;
    let total_records = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as i64;
    FlightInfo::new()
        .try_with_schema(fixture_schema(kind).as_ref())
        .map(|info| {
            info.with_descriptor(FlightDescriptor::new_path(fixture_descriptor(kind)))
                .with_endpoint(FlightEndpoint::new().with_ticket(Ticket {
                    ticket: Bytes::from(fixture_ticket_bytes(kind)),
                }))
                .with_total_records(total_records)
                .with_total_bytes(-1)
                .with_ordered(true)
        })
        .map_err(|err| format!("failed to build fixture FlightInfo: {err}").into())
}

fn schema_fields(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

fn schema_from_result(result: SchemaResult) -> Result<Schema, Box<dyn Error>> {
    Schema::try_from(result).map_err(|err| format!("failed to decode Flight schema: {err}").into())
}

fn duckdb_type_name(data_type: &DataType) -> &'static str {
    match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "VARCHAR",
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(TimeUnit::Second, None) => "TIMESTAMP_S",
        DataType::Timestamp(TimeUnit::Millisecond, None) => "TIMESTAMP_MS",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP",
        DataType::Timestamp(TimeUnit::Nanosecond, None) => "TIMESTAMP_NS",
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ",
        DataType::Decimal128(_, _) => "DECIMAL",
        DataType::List(_) | DataType::LargeList(_) => "LIST",
        DataType::Struct(_) => "STRUCT",
        _ => "unsupported",
    }
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let trimmed = value.trim();
    let hex = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
        .unwrap_or(trimmed);
    if !hex.len().is_multiple_of(2) {
        return Err("hex ticket values must have an even number of digits".into());
    }
    let mut output = Vec::with_capacity(hex.len() / 2);
    for (pair_index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_value(pair[0]).ok_or_else(|| {
            format!(
                "invalid hex digit '{}' at position {}",
                pair[0] as char,
                pair_index * 2
            )
        })?;
        let low = hex_value(pair[1]).ok_or_else(|| {
            format!(
                "invalid hex digit '{}' at position {}",
                pair[1] as char,
                pair_index * 2 + 1
            )
        })?;
        output.push((high << 4) | low);
    }
    Ok(output)
}

fn decode_ticket_bytes(ticket: &str, encoding: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    match encoding.to_ascii_lowercase().as_str() {
        "utf8" | "utf-8" | "text" => Ok(ticket.as_bytes().to_vec()),
        "hex" => decode_hex(ticket),
        "base64" => general_purpose::STANDARD
            .decode(ticket.trim())
            .map_err(|err| format!("invalid base64 ticket: {err}").into()),
        other => Err(format!("ticket_encoding must be utf8, hex, or base64, got {other}").into()),
    }
}

fn ticket_bytes_from_bind(bind: &BindInfo) -> Result<Vec<u8>, Box<dyn Error>> {
    let ticket = bind.get_parameter(1).to_string();
    let encoding = bind
        .get_named_parameter("ticket_encoding")
        .map(|value| value.to_string())
        .unwrap_or_else(|| "utf8".to_string());
    decode_ticket_bytes(&ticket, &encoding)
}

fn bytes_display(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(value)
            if value.chars().all(|character| {
                !character.is_control() || matches!(character, '\t' | '\n' | '\r')
            }) =>
        {
            value.to_string()
        }
        _ => format!("hex:{}", bytes_to_hex(bytes)),
    }
}

async fn remote_flight_info(
    location: &str,
    descriptor: &Descriptor,
    options: &FlightOptions,
) -> Result<FlightInfo, Box<dyn Error>> {
    let mut client = connect(location, options).await?;
    let request = with_auth(Request::new(descriptor.to_flight_descriptor()), options)?;
    client
        .get_flight_info(request)
        .await
        .map(|response| response.into_inner())
        .map_err(|status| format!("GetFlightInfo failed: {status}").into())
}

async fn remote_schema(
    location: &str,
    descriptor: &Descriptor,
    options: &FlightOptions,
) -> Result<Schema, Box<dyn Error>> {
    let mut client = connect(location, options).await?;
    let request = with_auth(Request::new(descriptor.to_flight_descriptor()), options)?;
    let result = client
        .get_schema(request)
        .await
        .map(|response| response.into_inner())
        .map_err(|status| format!("GetSchema failed: {status}"))?;
    schema_from_result(result)
}

async fn remote_list(
    location: &str,
    options: &FlightOptions,
) -> Result<Vec<FlightInfo>, Box<dyn Error>> {
    let mut client = connect(location, options).await?;
    let request = with_auth(
        Request::new(Criteria {
            expression: Bytes::new(),
        }),
        options,
    )?;
    let mut stream = client
        .list_flights(request)
        .await
        .map_err(|status| format!("ListFlights failed: {status}"))?
        .into_inner();
    let mut infos = Vec::new();
    while let Some(info) = stream.next().await {
        infos.push(info.map_err(|status| format!("ListFlights stream failed: {status}"))?);
    }
    Ok(infos)
}

#[derive(Debug, Clone)]
struct ScanEndpoint {
    location: String,
    ticket: Vec<u8>,
}

type BoxedBatchStream =
    Pin<Box<dyn Stream<Item = std::result::Result<RecordBatch, String>> + Send>>;

fn scan_endpoints_from_info(
    info: &FlightInfo,
    fallback_location: &str,
) -> Result<Vec<ScanEndpoint>, Box<dyn Error>> {
    let mut endpoints = Vec::new();
    for (index, endpoint) in info.endpoint.iter().enumerate() {
        let ticket = endpoint
            .ticket
            .as_ref()
            .ok_or_else(|| format!("Flight endpoint {index} did not include a ticket"))?;
        let endpoint_location = endpoint
            .location
            .first()
            .map(|location| location.uri.as_str())
            .filter(|uri| !uri.is_empty() && *uri != "arrow-flight-reuse-connection://?")
            .unwrap_or(fallback_location)
            .to_string();
        endpoints.push(ScanEndpoint {
            location: endpoint_location,
            ticket: ticket.ticket.to_vec(),
        });
    }
    if endpoints.is_empty() {
        return Err("FlightInfo did not include any readable endpoints".into());
    }
    Ok(endpoints)
}

async fn open_ticket_batch_stream(
    endpoint: ScanEndpoint,
    options: FlightOptions,
) -> std::result::Result<BoxedBatchStream, String> {
    let mut client = connect(&endpoint.location, &options)
        .await
        .map_err(|err| err.to_string())?;
    let request = with_auth(
        Request::new(Ticket {
            ticket: Bytes::from(endpoint.ticket),
        }),
        &options,
    )
    .map_err(|err| err.to_string())?;
    let data_stream = client
        .do_get(request)
        .await
        .map_err(|status| format!("DoGet failed: {status}"))?
        .into_inner()
        .map_err(Into::into);
    let batch_stream = FlightRecordBatchStream::new_from_flight_data(data_stream)
        .map(|batch| batch.map_err(|err| format!("failed to decode FlightData stream: {err}")));
    Ok(Box::pin(batch_stream))
}

fn add_columns(bind: &BindInfo, columns: &[ColumnSpec]) {
    for column in columns {
        bind.add_result_column(column.name, column.logical_type());
    }
}

fn write_rows(
    rows: &[Vec<Cell>],
    offset: &AtomicUsize,
    output: &mut DataChunkHandle,
) -> Result<(), Box<dyn Error>> {
    let start = offset.fetch_add(2048, Ordering::Relaxed);
    if start >= rows.len() {
        output.set_len(0);
        return Ok(());
    }
    let end = rows.len().min(start + 2048);
    for (row_index, row) in rows[start..end].iter().enumerate() {
        for (column_index, cell) in row.iter().enumerate() {
            let vector = output.flat_vector(column_index);
            match cell {
                Cell::String(value) => vector.insert(row_index, value),
                Cell::I64(value) => {
                    let mut vector = vector;
                    unsafe {
                        vector.as_mut_slice_with_len::<i64>(row_index + 1)[row_index] = *value;
                    }
                }
                Cell::Bool(value) => {
                    let mut vector = vector;
                    unsafe {
                        vector.as_mut_slice_with_len::<bool>(row_index + 1)[row_index] = *value;
                    }
                }
            }
        }
    }
    output.set_len(end - start);
    Ok(())
}

fn descriptor_to_cells(descriptor: Option<&FlightDescriptor>) -> (String, String) {
    let Some(descriptor) = descriptor else {
        return ("unknown".to_string(), "".to_string());
    };
    match descriptor.r#type() {
        arrow_flight::flight_descriptor::DescriptorType::Path => {
            ("path".to_string(), descriptor.path.join("/"))
        }
        arrow_flight::flight_descriptor::DescriptorType::Cmd => {
            ("cmd".to_string(), bytes_display(&descriptor.cmd))
        }
        arrow_flight::flight_descriptor::DescriptorType::Unknown => {
            ("unknown".to_string(), "".to_string())
        }
    }
}

struct FlightCheckVTab;

impl VTab for FlightCheckVTab {
    type InitData = RowsInitData;
    type BindData = RowsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let columns = [
            ColumnSpec {
                name: "check_name",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "status",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "message",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "detail",
                kind: ColumnKind::String,
            },
        ];
        add_columns(bind, &columns);
        let location = bind.get_parameter(0).to_string();
        let rows = if let Some(kind) = fixture_kind(&location) {
            vec![
                vec![
                    Cell::String("connect".to_string()),
                    Cell::String("ok".to_string()),
                    Cell::String("fixture service available".to_string()),
                    Cell::String(location),
                ],
                vec![
                    Cell::String("schema".to_string()),
                    Cell::String("ok".to_string()),
                    Cell::String(format!("{} fields", fixture_schema(kind).fields().len())),
                    Cell::String(schema_fields(fixture_schema(kind).as_ref()).join(",")),
                ],
            ]
        } else {
            let options = parse_options(bind)?;
            match runtime()?.block_on(remote_list(&location, &options)) {
                Ok(infos) => vec![vec![
                    Cell::String("connect".to_string()),
                    Cell::String("ok".to_string()),
                    Cell::String("Flight service reachable".to_string()),
                    Cell::String(format!("{} advertised flights", infos.len())),
                ]],
                Err(err) => vec![vec![
                    Cell::String("connect".to_string()),
                    Cell::String("error".to_string()),
                    Cell::String("Flight service check failed".to_string()),
                    Cell::String(err.to_string()),
                ]],
            }
        };
        Ok(RowsBindData { rows })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RowsInitData {
            offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        write_rows(
            &func.get_bind_data().rows,
            &func.get_init_data().offset,
            output,
        )
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(common_named_parameters())
    }
}

struct FlightListVTab;

impl VTab for FlightListVTab {
    type InitData = RowsInitData;
    type BindData = RowsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let columns = [
            ColumnSpec {
                name: "descriptor_type",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "descriptor",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "endpoint_count",
                kind: ColumnKind::I64,
            },
            ColumnSpec {
                name: "total_records",
                kind: ColumnKind::I64,
            },
            ColumnSpec {
                name: "total_bytes",
                kind: ColumnKind::I64,
            },
            ColumnSpec {
                name: "ordered",
                kind: ColumnKind::Bool,
            },
            ColumnSpec {
                name: "app_metadata",
                kind: ColumnKind::String,
            },
        ];
        add_columns(bind, &columns);
        let location = bind.get_parameter(0).to_string();
        let infos = if let Some(kind) = fixture_kind(&location) {
            vec![fixture_flight_info(kind)?]
        } else {
            let options = parse_options(bind)?;
            runtime()?.block_on(remote_list(&location, &options))?
        };
        let rows = infos
            .iter()
            .map(|info| {
                let (descriptor_type, descriptor) =
                    descriptor_to_cells(info.flight_descriptor.as_ref());
                vec![
                    Cell::String(descriptor_type),
                    Cell::String(descriptor),
                    Cell::I64(info.endpoint.len() as i64),
                    Cell::I64(info.total_records),
                    Cell::I64(info.total_bytes),
                    Cell::Bool(info.ordered),
                    Cell::String(bytes_display(&info.app_metadata)),
                ]
            })
            .collect();
        Ok(RowsBindData { rows })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RowsInitData {
            offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        write_rows(
            &func.get_bind_data().rows,
            &func.get_init_data().offset,
            output,
        )
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        Some(common_named_parameters())
    }
}

struct FlightSchemaVTab;

impl VTab for FlightSchemaVTab {
    type InitData = RowsInitData;
    type BindData = RowsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let columns = [
            ColumnSpec {
                name: "field_index",
                kind: ColumnKind::I64,
            },
            ColumnSpec {
                name: "field_name",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "arrow_type",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "duckdb_type",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "nullable",
                kind: ColumnKind::Bool,
            },
            ColumnSpec {
                name: "metadata",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "supported",
                kind: ColumnKind::Bool,
            },
        ];
        add_columns(bind, &columns);
        let location = bind.get_parameter(0).to_string();
        let descriptor = descriptor_from_bind(bind, "path")?;
        let schema = if let Some(kind) = fixture_kind(&location) {
            fixture_schema(kind).as_ref().clone()
        } else {
            let options = parse_options(bind)?;
            runtime()?.block_on(remote_schema(&location, &descriptor, &options))?
        };
        let rows = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let supported = to_duckdb_logical_type(field.data_type()).is_ok();
                let duckdb_type = if supported {
                    duckdb_type_name(field.data_type()).to_string()
                } else {
                    "unsupported".to_string()
                };
                vec![
                    Cell::I64(index as i64),
                    Cell::String(field.name().to_string()),
                    Cell::String(format!("{:?}", field.data_type())),
                    Cell::String(duckdb_type.clone()),
                    Cell::Bool(field.is_nullable()),
                    Cell::String(format!("{:?}", field.metadata())),
                    Cell::Bool(supported),
                ]
            })
            .collect();
        Ok(RowsBindData { rows })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RowsInitData {
            offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        write_rows(
            &func.get_bind_data().rows,
            &func.get_init_data().offset,
            output,
        )
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Any),
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let mut params = common_named_parameters();
        params.push((
            "descriptor_type".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ));
        Some(params)
    }
}

struct FlightInfoVTab;

impl VTab for FlightInfoVTab {
    type InitData = RowsInitData;
    type BindData = RowsBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let columns = [
            ColumnSpec {
                name: "endpoint_index",
                kind: ColumnKind::I64,
            },
            ColumnSpec {
                name: "location",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "ticket",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "ticket_hex",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "ticket_size",
                kind: ColumnKind::I64,
            },
            ColumnSpec {
                name: "expiration_time",
                kind: ColumnKind::String,
            },
            ColumnSpec {
                name: "total_records",
                kind: ColumnKind::I64,
            },
            ColumnSpec {
                name: "total_bytes",
                kind: ColumnKind::I64,
            },
            ColumnSpec {
                name: "app_metadata",
                kind: ColumnKind::String,
            },
        ];
        add_columns(bind, &columns);
        let location = bind.get_parameter(0).to_string();
        let descriptor = descriptor_from_bind(bind, "path")?;
        let info = if let Some(kind) = fixture_kind(&location) {
            fixture_flight_info(kind)?
        } else {
            let options = parse_options(bind)?;
            runtime()?.block_on(remote_flight_info(&location, &descriptor, &options))?
        };
        let rows = info
            .endpoint
            .iter()
            .enumerate()
            .map(|(index, endpoint)| {
                let ticket = endpoint
                    .ticket
                    .as_ref()
                    .map(|ticket| ticket.ticket.as_ref());
                vec![
                    Cell::I64(index as i64),
                    Cell::String(
                        endpoint
                            .location
                            .first()
                            .map(|location| location.uri.clone())
                            .unwrap_or_default(),
                    ),
                    Cell::String(ticket.map(bytes_display).unwrap_or_default()),
                    Cell::String(ticket.map(bytes_to_hex).unwrap_or_default()),
                    Cell::I64(ticket.map(|ticket| ticket.len() as i64).unwrap_or_default()),
                    Cell::String(format!("{:?}", endpoint.expiration_time)),
                    Cell::I64(info.total_records),
                    Cell::I64(info.total_bytes),
                    Cell::String(bytes_display(&info.app_metadata)),
                ]
            })
            .collect();
        Ok(RowsBindData { rows })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(RowsInitData {
            offset: AtomicUsize::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        write_rows(
            &func.get_bind_data().rows,
            &func.get_init_data().offset,
            output,
        )
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Any),
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let mut params = common_named_parameters();
        params.push((
            "descriptor_type".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ));
        Some(params)
    }
}

struct FlightScanBindData {
    source: FlightScanSource,
}

enum FlightScanSource {
    Prebuffered(Vec<RecordBatch>),
    RemoteEndpoints {
        endpoints: Vec<ScanEndpoint>,
        options: FlightOptions,
    },
}

struct FlightScanInitData {
    runtime: Runtime,
    state: Mutex<FlightScanState>,
}

enum FlightScanState {
    NotStarted,
    Prebuffered {
        batch_index: usize,
    },
    Remote {
        endpoint_index: usize,
        stream: Option<BoxedBatchStream>,
    },
    Done,
}

fn next_scan_batch(
    bind_data: &FlightScanBindData,
    init_data: &FlightScanInitData,
) -> Result<Option<RecordBatch>, Box<dyn Error>> {
    let mut state = init_data
        .state
        .lock()
        .map_err(|err| format!("Flight scan state lock failed: {err}"))?;

    loop {
        if matches!(*state, FlightScanState::NotStarted) {
            *state = match &bind_data.source {
                FlightScanSource::Prebuffered(_) => FlightScanState::Prebuffered { batch_index: 0 },
                FlightScanSource::RemoteEndpoints { .. } => FlightScanState::Remote {
                    endpoint_index: 0,
                    stream: None,
                },
            };
        }

        match &mut *state {
            FlightScanState::NotStarted => unreachable!(),
            FlightScanState::Done => return Ok(None),
            FlightScanState::Prebuffered { batch_index } => {
                let FlightScanSource::Prebuffered(batches) = &bind_data.source else {
                    return Err("Flight scan state/source mismatch".into());
                };
                let Some(batch) = batches.get(*batch_index).cloned() else {
                    *state = FlightScanState::Done;
                    return Ok(None);
                };
                *batch_index += 1;
                return Ok(Some(batch));
            }
            FlightScanState::Remote {
                endpoint_index,
                stream,
            } => {
                let FlightScanSource::RemoteEndpoints { endpoints, options } = &bind_data.source
                else {
                    return Err("Flight scan state/source mismatch".into());
                };

                if let Some(current_stream) = stream.as_mut() {
                    match init_data.runtime.block_on(current_stream.next()) {
                        Some(Ok(batch)) => return Ok(Some(batch)),
                        Some(Err(err)) => return Err(err.into()),
                        None => {
                            *stream = None;
                            continue;
                        }
                    }
                }

                let Some(endpoint) = endpoints.get(*endpoint_index).cloned() else {
                    *state = FlightScanState::Done;
                    return Ok(None);
                };
                *endpoint_index += 1;
                *stream = Some(
                    init_data
                        .runtime
                        .block_on(open_ticket_batch_stream(endpoint, options.clone()))
                        .map_err(|err| format!("failed to open Flight endpoint stream: {err}"))?,
                );
            }
        }
    }
}

struct FlightScanVTab;

impl VTab for FlightScanVTab {
    type InitData = FlightScanInitData;
    type BindData = FlightScanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let location = bind.get_parameter(0).to_string();
        let descriptor = descriptor_from_bind(bind, "path")?;
        let source = if let Some(kind) = fixture_kind(&location) {
            FlightScanSource::Prebuffered(fixture_batches(kind)?)
        } else {
            let options = parse_options(bind)?;
            let info = runtime()?.block_on(remote_flight_info(&location, &descriptor, &options))?;
            let schema = Schema::try_from(info.clone())
                .map_err(|err| format!("failed to decode FlightInfo schema: {err}"))?;
            for field in schema.fields() {
                bind.add_result_column(field.name(), to_duckdb_logical_type(field.data_type())?);
            }
            return Ok(FlightScanBindData {
                source: FlightScanSource::RemoteEndpoints {
                    endpoints: scan_endpoints_from_info(&info, &location)?,
                    options,
                },
            });
        };
        let FlightScanSource::Prebuffered(batches) = &source else {
            unreachable!();
        };
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or("Flight scan returned no batches, so the schema cannot be bound")?;
        for field in schema.fields() {
            bind.add_result_column(field.name(), to_duckdb_logical_type(field.data_type())?);
        }
        Ok(FlightScanBindData { source })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(FlightScanInitData {
            runtime: runtime()?,
            state: Mutex::new(FlightScanState::NotStarted),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let Some(batch) = next_scan_batch(func.get_bind_data(), func.get_init_data())? else {
            output.set_len(0);
            return Ok(());
        };
        record_batch_to_duckdb_data_chunk(&batch, output)
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Any),
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let mut params = common_named_parameters();
        params.push((
            "descriptor_type".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ));
        Some(params)
    }
}

struct FlightTicketScanVTab;

impl VTab for FlightTicketScanVTab {
    type InitData = FlightScanInitData;
    type BindData = FlightScanBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let location = bind.get_parameter(0).to_string();
        let ticket = ticket_bytes_from_bind(bind)?;
        let options = parse_options(bind)?;
        if let Some(schema_descriptor) = schema_descriptor_from_bind(bind)? {
            let schema = if let Some(kind) = fixture_kind(&location) {
                fixture_schema(kind).as_ref().clone()
            } else {
                runtime()?.block_on(remote_schema(&location, &schema_descriptor, &options))?
            };
            for field in schema.fields() {
                bind.add_result_column(field.name(), to_duckdb_logical_type(field.data_type())?);
            }
            if let Some(kind) = fixture_kind(&location) {
                validate_fixture_ticket(kind, &ticket)?;
                return Ok(FlightScanBindData {
                    source: FlightScanSource::Prebuffered(fixture_batches(kind)?),
                });
            }
            return Ok(FlightScanBindData {
                source: FlightScanSource::RemoteEndpoints {
                    endpoints: vec![ScanEndpoint { location, ticket }],
                    options,
                },
            });
        }
        if let Some(kind) = fixture_kind(&location) {
            validate_fixture_ticket(kind, &ticket)?;
            let batches = fixture_batches(kind)?;
            for field in batches[0].schema().fields() {
                bind.add_result_column(field.name(), to_duckdb_logical_type(field.data_type())?);
            }
            return Ok(FlightScanBindData {
                source: FlightScanSource::Prebuffered(batches),
            });
        }
        let batches = runtime()?.block_on(async {
            let mut client = connect(&location, &options).await?;
            let request = with_auth(
                Request::new(Ticket {
                    ticket: Bytes::from(ticket),
                }),
                &options,
            )?;
            let data_stream = client
                .do_get(request)
                .await
                .map_err(|status| format!("DoGet failed: {status}"))?
                .into_inner()
                .map_err(Into::into);
            let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(data_stream);
            let mut batches = Vec::new();
            while let Some(batch) = batch_stream.next().await {
                batches.push(
                    batch.map_err(|err| format!("failed to decode FlightData stream: {err}"))?,
                );
            }
            Ok::<_, Box<dyn Error>>(batches)
        })?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or("Flight ticket scan returned no batches, so the schema cannot be bound")?;
        for field in schema.fields() {
            bind.add_result_column(field.name(), to_duckdb_logical_type(field.data_type())?);
        }
        Ok(FlightScanBindData {
            source: FlightScanSource::Prebuffered(batches),
        })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(FlightScanInitData {
            runtime: runtime()?,
            state: Mutex::new(FlightScanState::NotStarted),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let Some(batch) = next_scan_batch(func.get_bind_data(), func.get_init_data())? else {
            output.set_len(0);
            return Ok(());
        };
        record_batch_to_duckdb_data_chunk(&batch, output)
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        let mut params = common_named_parameters();
        params.push((
            "schema_descriptor".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Any),
        ));
        params.push((
            "schema_descriptor_type".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ));
        params.push((
            "ticket_encoding".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ));
        Some(params)
    }
}

fn common_named_parameters() -> Vec<(String, LogicalTypeHandle)> {
    vec![
        (
            "bearer_token".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "authorization_header".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ),
        (
            "max_message_size".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::UBigint),
        ),
        (
            "timeout_ms".to_string(),
            LogicalTypeHandle::from(LogicalTypeId::UBigint),
        ),
    ]
}

fn register_flight_functions(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<FlightCheckVTab>("flight_check")?;
    con.register_table_function::<FlightListVTab>("flight_list")?;
    con.register_table_function::<FlightInfoVTab>("flight_info")?;
    con.register_table_function::<FlightSchemaVTab>("flight_schema")?;
    con.register_table_function::<FlightScanVTab>("flight_scan")?;
    con.register_table_function::<FlightScanVTab>("flight")?;
    con.register_table_function::<FlightTicketScanVTab>("flight_scan_ticket")?;
    Ok(())
}

#[duckdb_entrypoint_c_api]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    register_flight_functions(&con)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;

    use arrow_array::Array;
    use arrow_flight::{
        encode::FlightDataEncoderBuilder,
        flight_service_server::{FlightService, FlightServiceServer},
        Action, ActionType, Empty, FlightData, HandshakeRequest, HandshakeResponse, PollInfo,
        PutResult, SchemaAsIpc,
    };
    use arrow_ipc::writer::IpcWriteOptions;
    use futures::{stream, Stream};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{transport::Server, Response, Status, Streaming};

    #[test]
    fn parses_path_descriptors() {
        assert_eq!(
            parse_path_descriptor("/warehouse/events/"),
            vec!["warehouse", "events"]
        );
    }

    #[test]
    fn fixture_batches_are_real_arrow_batches() {
        let batches = fixture_batches(FixtureKind::Events).expect("fixture batches");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 4);
        assert_eq!(
            schema_fields(batches[0].schema().as_ref()),
            vec!["event_id", "event_type", "amount", "created_at"]
        );
    }

    #[test]
    fn decodes_text_hex_and_base64_tickets() {
        assert_eq!(
            decode_ticket_bytes("fixture-events", "utf8").expect("utf8 ticket"),
            b"fixture-events"
        );
        assert_eq!(
            decode_ticket_bytes("0x00ff41", "hex").expect("hex ticket"),
            vec![0, 255, 65]
        );
        assert_eq!(
            decode_ticket_bytes("AP9B", "base64").expect("base64 ticket"),
            vec![0, 255, 65]
        );
        assert!(decode_ticket_bytes("abc", "hex").is_err());
    }

    #[derive(Clone, Copy)]
    struct TinyFlightService {
        kind: FixtureKind,
        binary_tickets: bool,
    }

    impl TinyFlightService {
        fn new(kind: FixtureKind) -> Self {
            Self {
                kind,
                binary_tickets: false,
            }
        }

        fn with_binary_tickets(kind: FixtureKind) -> Self {
            Self {
                kind,
                binary_tickets: true,
            }
        }

        fn endpoint_ticket(&self, suffix: u8) -> Vec<u8> {
            if self.binary_tickets {
                vec![0x00, 0xff, suffix]
            } else {
                format!("{}-{}", fixture_ticket(self.kind), suffix as char).into_bytes()
            }
        }
    }

    impl Default for TinyFlightService {
        fn default() -> Self {
            Self::new(FixtureKind::Events)
        }
    }

    type StatusStream<T> =
        Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send + 'static>>;

    fn partitioned_fixture_flight_info(
        service: TinyFlightService,
    ) -> Result<FlightInfo, Box<dyn Error>> {
        let kind = service.kind;
        let batches = fixture_batches(kind)?;
        let total_records = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as i64;
        FlightInfo::new()
            .try_with_schema(fixture_schema(kind).as_ref())
            .map(|info| {
                info.with_descriptor(FlightDescriptor::new_path(fixture_descriptor(kind)))
                    .with_endpoint(FlightEndpoint::new().with_ticket(Ticket {
                        ticket: Bytes::from(service.endpoint_ticket(b'a')),
                    }))
                    .with_endpoint(FlightEndpoint::new().with_ticket(Ticket {
                        ticket: Bytes::from(service.endpoint_ticket(b'b')),
                    }))
                    .with_total_records(total_records)
                    .with_total_bytes(-1)
                    .with_ordered(true)
            })
            .map_err(|err| format!("failed to build partitioned fixture FlightInfo: {err}").into())
    }

    #[tonic::async_trait]
    impl FlightService for TinyFlightService {
        type HandshakeStream = StatusStream<HandshakeResponse>;
        type ListFlightsStream = StatusStream<FlightInfo>;
        type DoGetStream = StatusStream<FlightData>;
        type DoPutStream = StatusStream<PutResult>;
        type DoExchangeStream = StatusStream<FlightData>;
        type DoActionStream = StatusStream<arrow_flight::Result>;
        type ListActionsStream = StatusStream<ActionType>;

        async fn handshake(
            &self,
            _request: Request<Streaming<HandshakeRequest>>,
        ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented(
                "handshake is not implemented by fixture",
            ))
        }

        async fn list_flights(
            &self,
            _request: Request<Criteria>,
        ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
            let info = partitioned_fixture_flight_info(*self)
                .map_err(|err| Status::internal(err.to_string()))?;
            Ok(Response::new(Box::pin(stream::iter(vec![Ok(info)]))))
        }

        async fn get_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<FlightInfo>, Status> {
            let info = partitioned_fixture_flight_info(*self)
                .map_err(|err| Status::internal(err.to_string()))?;
            Ok(Response::new(info))
        }

        async fn poll_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<PollInfo>, Status> {
            Err(Status::unimplemented(
                "poll_flight_info is not implemented by fixture",
            ))
        }

        async fn get_schema(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<SchemaResult>, Status> {
            let options = IpcWriteOptions::default();
            let schema_result = SchemaAsIpc::new(fixture_schema(self.kind).as_ref(), &options)
                .try_into()
                .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?;
            Ok(Response::new(schema_result))
        }

        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
            let ticket = request.into_inner().ticket.to_vec();
            let mut batches =
                fixture_batches(self.kind).map_err(|err| Status::internal(err.to_string()))?;
            let batch = batches
                .pop()
                .ok_or_else(|| Status::internal("fixture had no batches"))?;
            let split = batch.num_rows().div_ceil(2);
            let first_ticket = self.endpoint_ticket(b'a');
            let second_ticket = self.endpoint_ticket(b'b');
            let batches = if ticket == first_ticket {
                vec![batch.slice(0, split)]
            } else if ticket == second_ticket {
                vec![batch.slice(split, batch.num_rows() - split)]
            } else {
                return Err(Status::not_found(format!(
                    "unknown fixture ticket: {}",
                    bytes_display(&ticket)
                )));
            };
            let input = stream::iter(batches.into_iter().map(Ok));
            let output = FlightDataEncoderBuilder::new()
                .with_schema(fixture_schema(self.kind))
                .build(input)
                .map(|item| item.map_err(|err| Status::internal(err.to_string())));
            Ok(Response::new(Box::pin(output)))
        }

        async fn do_put(
            &self,
            _request: Request<Streaming<FlightData>>,
        ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented(
                "do_put is not implemented by fixture",
            ))
        }

        async fn do_exchange(
            &self,
            _request: Request<Streaming<FlightData>>,
        ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented(
                "do_exchange is not implemented by fixture",
            ))
        }

        async fn do_action(
            &self,
            _request: Request<Action>,
        ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
            Err(Status::unimplemented(
                "do_action is not implemented by fixture",
            ))
        }

        async fn list_actions(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
            Ok(Response::new(Box::pin(stream::empty())))
        }
    }

    #[tokio::test]
    async fn remote_client_reads_real_flight_fixture() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture server");
        let address = listener.local_addr().expect("fixture address");
        let incoming = TcpListenerStream::new(listener);
        let server = Server::builder()
            .add_service(FlightServiceServer::new(TinyFlightService::default()))
            .serve_with_incoming(incoming);
        tokio::spawn(server);

        let location = format!("grpc://{address}");
        let options = FlightOptions::default();
        let infos = remote_list(&location, &options)
            .await
            .expect("list flights");
        assert_eq!(infos.len(), 1);
        let listed_schema = Schema::try_from(infos[0].clone()).expect("schema");
        assert_eq!(listed_schema.fields().len(), 4);

        let descriptor = Descriptor::Path(vec!["warehouse".to_string(), "events".to_string()]);
        let schema = remote_schema(&location, &descriptor, &options)
            .await
            .expect("get schema");
        assert_eq!(
            schema_fields(&schema),
            vec!["event_id", "event_type", "amount", "created_at"]
        );

        let info = remote_flight_info(&location, &descriptor, &options)
            .await
            .expect("get flight info");
        let endpoints = scan_endpoints_from_info(&info, &location).expect("scan endpoints");
        assert_eq!(endpoints.len(), 2);
        let mut batches = Vec::new();
        for endpoint in endpoints {
            let mut stream = open_ticket_batch_stream(endpoint, options.clone())
                .await
                .expect("open endpoint stream");
            while let Some(batch) = stream.next().await {
                batches.push(batch.expect("stream batch"));
            }
        }
        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);
    }

    #[tokio::test]
    async fn remote_descriptor_scan_preserves_binary_endpoint_tickets() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture server");
        let address = listener.local_addr().expect("fixture address");
        let incoming = TcpListenerStream::new(listener);
        let server = Server::builder()
            .add_service(FlightServiceServer::new(
                TinyFlightService::with_binary_tickets(FixtureKind::Events),
            ))
            .serve_with_incoming(incoming);
        tokio::spawn(server);

        let location = format!("grpc://{address}");
        let options = FlightOptions::default();
        let descriptor = Descriptor::Path(vec!["warehouse".to_string(), "events".to_string()]);
        let info = remote_flight_info(&location, &descriptor, &options)
            .await
            .expect("get flight info");
        let endpoints = scan_endpoints_from_info(&info, &location).expect("scan endpoints");
        assert_eq!(endpoints.len(), 2);
        assert_eq!(endpoints[0].ticket, vec![0x00, 0xff, b'a']);
        assert_eq!(bytes_display(&endpoints[0].ticket), "hex:00ff61");

        let mut row_count = 0;
        for endpoint in endpoints {
            let mut stream = open_ticket_batch_stream(endpoint, options.clone())
                .await
                .expect("open endpoint stream");
            while let Some(batch) = stream.next().await {
                row_count += batch.expect("stream batch").num_rows();
            }
        }
        assert_eq!(row_count, 4);
    }

    #[tokio::test]
    async fn scan_state_reads_real_world_weather_flight_streams() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture server");
        let address = listener.local_addr().expect("fixture address");
        let incoming = TcpListenerStream::new(listener);
        let server = Server::builder()
            .add_service(FlightServiceServer::new(TinyFlightService::new(
                FixtureKind::SeattleWeather,
            )))
            .serve_with_incoming(incoming);
        tokio::spawn(server);

        let location = format!("grpc://{address}");
        let options = FlightOptions::default();
        let descriptor = Descriptor::Path(fixture_descriptor(FixtureKind::SeattleWeather));
        let info = remote_flight_info(&location, &descriptor, &options)
            .await
            .expect("get flight info");
        let endpoints = scan_endpoints_from_info(&info, &location).expect("scan endpoints");

        let (weather_labels, precipitation_sum) = tokio::task::spawn_blocking(
            move || -> std::result::Result<(Vec<String>, f64), String> {
                let bind_data = FlightScanBindData {
                    source: FlightScanSource::RemoteEndpoints { endpoints, options },
                };
                let init_data = FlightScanInitData {
                    runtime: runtime().map_err(|err| err.to_string())?,
                    state: Mutex::new(FlightScanState::NotStarted),
                };

                let mut weather_labels = Vec::new();
                let mut precipitation_sum = 0.0;
                while let Some(batch) =
                    next_scan_batch(&bind_data, &init_data).map_err(|err| err.to_string())?
                {
                    let precipitation_column = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| "precipitation column was not Float64".to_string())?;
                    precipitation_sum += (0..precipitation_column.len())
                        .map(|index| precipitation_column.value(index))
                        .sum::<f64>();

                    let weather_column = batch
                        .column(5)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| "weather column was not Utf8".to_string())?;
                    weather_labels.extend(
                        (0..weather_column.len())
                            .map(|index| weather_column.value(index).to_string()),
                    );
                }

                Ok((weather_labels, precipitation_sum))
            },
        )
        .await
        .expect("scan task panicked")
        .expect("scan task failed");

        assert_eq!(
            weather_labels,
            vec!["drizzle", "rain", "rain", "rain", "rain", "sun"]
        );
        assert!((precipitation_sum - 33.3).abs() < 1e-9);
    }

    #[tokio::test]
    async fn scan_state_reads_real_world_bio_flight_streams() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture server");
        let address = listener.local_addr().expect("fixture address");
        let incoming = TcpListenerStream::new(listener);
        let server = Server::builder()
            .add_service(FlightServiceServer::new(TinyFlightService::new(
                FixtureKind::PalmerPenguins,
            )))
            .serve_with_incoming(incoming);
        tokio::spawn(server);

        let location = format!("grpc://{address}");
        let options = FlightOptions::default();
        let descriptor = Descriptor::Path(fixture_descriptor(FixtureKind::PalmerPenguins));
        let info = remote_flight_info(&location, &descriptor, &options)
            .await
            .expect("get flight info");
        let endpoints = scan_endpoints_from_info(&info, &location).expect("scan endpoints");

        let (species, body_mass_sum) = tokio::task::spawn_blocking(
            move || -> std::result::Result<(Vec<String>, i64), String> {
                let bind_data = FlightScanBindData {
                    source: FlightScanSource::RemoteEndpoints { endpoints, options },
                };
                let init_data = FlightScanInitData {
                    runtime: runtime().map_err(|err| err.to_string())?,
                    state: Mutex::new(FlightScanState::NotStarted),
                };

                let mut species = Vec::new();
                let mut body_mass_sum = 0;
                while let Some(batch) =
                    next_scan_batch(&bind_data, &init_data).map_err(|err| err.to_string())?
                {
                    let species_column = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| "species column was not Utf8".to_string())?;
                    species.extend(
                        (0..species_column.len())
                            .map(|index| species_column.value(index).to_string()),
                    );

                    let body_mass_column = batch
                        .column(5)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .ok_or_else(|| "body_mass_g column was not Int64".to_string())?;
                    body_mass_sum += (0..body_mass_column.len())
                        .filter(|index| !body_mass_column.is_null(*index))
                        .map(|index| body_mass_column.value(index))
                        .sum::<i64>();
                }

                Ok((species, body_mass_sum))
            },
        )
        .await
        .expect("scan task panicked")
        .expect("scan task failed");

        assert_eq!(species.len(), 9);
        assert_eq!(
            species,
            vec![
                "Adelie",
                "Adelie",
                "Adelie",
                "Gentoo",
                "Gentoo",
                "Gentoo",
                "Chinstrap",
                "Chinstrap",
                "Chinstrap"
            ]
        );
        assert_eq!(body_mass_sum, 36500);
    }
}
