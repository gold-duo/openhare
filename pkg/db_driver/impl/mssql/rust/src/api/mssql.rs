use crate::frb_generated::StreamSink;
use chrono::{Duration, NaiveDate, NaiveDateTime};
use futures_util::TryStreamExt;
use std::borrow::Cow;
use tiberius::{
    AuthMethod, Client, Column, ColumnData, ColumnType, Config, EncryptionLevel, QueryItem,
};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use url::Url;

pub enum QueryValue {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    DateTime(i64),
}

impl QueryValue {
    #[flutter_rust_bridge::frb(ignore)]
    pub fn from_columndata(value: ColumnData<'static>) -> Self {
        match value {
            ColumnData::U8(v) => v.map(|x| QueryValue::UInt(x as u64)).unwrap_or(QueryValue::NULL),
            ColumnData::I16(v) => v.map(|x| QueryValue::Int(x as i64)).unwrap_or(QueryValue::NULL),
            ColumnData::I32(v) => v.map(|x| QueryValue::Int(x as i64)).unwrap_or(QueryValue::NULL),
            ColumnData::I64(v) => v.map(QueryValue::Int).unwrap_or(QueryValue::NULL),
            ColumnData::F32(v) => v.map(QueryValue::Float).unwrap_or(QueryValue::NULL),
            ColumnData::F64(v) => v.map(QueryValue::Double).unwrap_or(QueryValue::NULL),
            ColumnData::Bit(v) => v
                .map(|b| QueryValue::Int(if b { 1 } else { 0 }))
                .unwrap_or(QueryValue::NULL),
            ColumnData::String(v) => v
                .map(|s| QueryValue::Bytes(s.as_bytes().to_vec()))
                .unwrap_or(QueryValue::NULL),
            ColumnData::Guid(v) => v
                .map(|g| QueryValue::Bytes(g.to_string().into_bytes()))
                .unwrap_or(QueryValue::NULL),
            ColumnData::Binary(v) => v
                .map(|b| QueryValue::Bytes(b.to_vec()))
                .unwrap_or(QueryValue::NULL),
            ColumnData::Numeric(v) => v
                .map(|n| QueryValue::Bytes(n.to_string().into_bytes()))
                .unwrap_or(QueryValue::NULL),
            ColumnData::Xml(v) => v
                .map(|x| QueryValue::Bytes(format!("{:?}", x).into_bytes()))
                .unwrap_or(QueryValue::NULL),
            ColumnData::DateTime(v) => v
                .map(|dt| QueryValue::DateTime(datetime_to_epoch_millis(dt)))
                .unwrap_or(QueryValue::NULL),
            ColumnData::SmallDateTime(v) => v
                .map(|dt| QueryValue::DateTime(smalldatetime_to_epoch_millis(dt)))
                .unwrap_or(QueryValue::NULL),
            ColumnData::Time(v) => v
                .map(|t| QueryValue::DateTime(time_to_millis_since_midnight(t)))
                .unwrap_or(QueryValue::NULL),
            ColumnData::Date(v) => v
                .map(|d| QueryValue::DateTime(date_to_epoch_millis(d)))
                .unwrap_or(QueryValue::NULL),
            ColumnData::DateTime2(v) => v
                .map(|dt| QueryValue::DateTime(datetime2_to_epoch_millis(dt)))
                .unwrap_or(QueryValue::NULL),
            ColumnData::DateTimeOffset(v) => v
                .map(|dt| QueryValue::DateTime(datetimeoffset_to_epoch_millis(dt)))
                .unwrap_or(QueryValue::NULL),
        }
    }
}

pub enum QueryStreamItem {
    Header(QueryHeader),
    Row(QueryRow),
    Error(String),
}

pub struct QueryHeader {
    pub columns: Vec<QueryColumn>,
    pub affected_rows: u64,
}

pub struct QueryColumn {
    pub name: String,
    pub column_type: u8,
}

impl QueryColumn {
    #[flutter_rust_bridge::frb(ignore)]
    pub fn from_column(col: &Column) -> Self {
        QueryColumn {
            name: col.name().to_string(),
            column_type: column_type_to_u8(col.column_type()),
        }
    }
}

pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

pub struct ConnWrapper {
    client: Client<Compat<TcpStream>>,
}

impl ConnWrapper {
    pub async fn open(dsn: &str) -> Result<Self, String> {
        let url = Url::parse(dsn).map_err(|e| e.to_string())?;
        if url.scheme() != "mssql" && url.scheme() != "sqlserver" {
            return Err(format!("unsupported dsn scheme: {}", url.scheme()));
        }

        let host = url
            .host_str()
            .ok_or_else(|| "dsn missing host".to_string())?
            .to_string();
        let port = url.port().unwrap_or(1433) as u16;
        let database = url.path().trim_start_matches('/').to_string();
        let database = if database.is_empty() {
            "master".to_string()
        } else {
            database
        };

        let username = url.username().to_string();
        if username.is_empty() {
            return Err("dsn missing username".to_string());
        }
        let password = url.password().unwrap_or("").to_string();

        let mut config = Config::new();
        config.host(host);
        config.port(port);
        config.database(database);
        config.authentication(AuthMethod::sql_server(username, password));
        config.application_name("openhare");

        let mut encrypt: Option<bool> = None;
        let mut trust_server_cert: Option<bool> = None;
        for (k, v) in url.query_pairs() {
            let key = k.to_ascii_lowercase();
            let val = v.to_ascii_lowercase();
            let as_bool = match val.as_str() {
                "true" | "yes" | "1" => Some(true),
                "false" | "no" | "0" => Some(false),
                _ => None,
            };
            match key.as_str() {
                "encrypt" => encrypt = as_bool,
                "trustservercertificate" => trust_server_cert = as_bool,
                _ => {}
            }
        }

        match encrypt {
            Some(true) | None => config.encryption(EncryptionLevel::Required),
            Some(false) => config.encryption(EncryptionLevel::Off),
        }
        if trust_server_cert == Some(true) {
            config.trust_cert();
        }

        let tcp = TcpStream::connect(config.get_addr())
            .await
            .map_err(|e| e.to_string())?;
        tcp.set_nodelay(true).map_err(|e| e.to_string())?;

        let client = Client::connect(config, tcp.compat_write())
            .await
            .map_err(|e| e.to_string())?;
        Ok(Self { client })
    }

    pub async fn query(
        &mut self,
        query: &str,
        sink: StreamSink<QueryStreamItem>,
    ) -> Result<(), String> {
        let mut stream = match self
            .client
            .query(Cow::Borrowed(query), &[] as &[&dyn tiberius::ToSql])
            .await
        {
            Ok(s) => s,
            Err(e) => {
                let _ = sink.add(QueryStreamItem::Error(e.to_string()));
                return Ok(());
            }
        };

        let mut sent_header = false;
        while let Some(item) = stream.try_next().await.map_err(|e| e.to_string())? {
            match item {
                QueryItem::Metadata(meta) => {
                    if sent_header {
                        let _ = sink.add(QueryStreamItem::Error(
                            "Multiple result sets are not supported".to_string(),
                        ));
                        return Ok(());
                    }
                    let columns = meta.columns().iter().map(QueryColumn::from_column).collect();
                    if sink
                        .add(QueryStreamItem::Header(QueryHeader {
                            columns,
                            affected_rows: 0, // todo: get affected rows
                        }))
                        .is_err()
                    {
                        return Ok(());
                    }
                    sent_header = true;
                }
                QueryItem::Row(row) => {
                    if !sent_header {
                        let columns = row.columns().iter().map(QueryColumn::from_column).collect();
                        let _ = sink.add(QueryStreamItem::Header(QueryHeader {
                            columns,
                            affected_rows: 0,
                        }));
                        sent_header = true;
                    }

                    let values = row.into_iter().map(QueryValue::from_columndata).collect();
                    if sink.add(QueryStreamItem::Row(QueryRow { values })).is_err() {
                        return Ok(());
                    }
                }
            }
        }

        if !sent_header {
            let _ = sink.add(QueryStreamItem::Header(QueryHeader {
                columns: vec![],
                affected_rows: 0,
            }));
        }

        Ok(())
    }

    pub async fn close(self) -> Result<(), String> {
        self.client.close().await.map_err(|e| e.to_string())
    }
}

fn column_type_to_u8(t: ColumnType) -> u8 {
    match t {
        ColumnType::Null => 0,
        ColumnType::Bit => 1,
        ColumnType::Int1 => 2,
        ColumnType::Int2 => 3,
        ColumnType::Int4 => 4,
        ColumnType::Int8 => 5,
        ColumnType::Datetime4 => 6,
        ColumnType::Float4 => 7,
        ColumnType::Float8 => 8,
        ColumnType::Money => 9,
        ColumnType::Datetime => 10,
        ColumnType::Money4 => 11,
        ColumnType::Guid => 12,
        ColumnType::Intn => 13,
        ColumnType::Bitn => 14,
        ColumnType::Decimaln => 15,
        ColumnType::Numericn => 16,
        ColumnType::Floatn => 17,
        ColumnType::Datetimen => 18,
        ColumnType::Daten => 19,
        ColumnType::Timen => 20,
        ColumnType::Datetime2 => 21,
        ColumnType::DatetimeOffsetn => 22,
        ColumnType::BigVarBin => 23,
        ColumnType::BigVarChar => 24,
        ColumnType::BigBinary => 25,
        ColumnType::BigChar => 26,
        ColumnType::NVarchar => 27,
        ColumnType::NChar => 28,
        ColumnType::Xml => 29,
        ColumnType::Udt => 30,
        ColumnType::Text => 31,
        ColumnType::Image => 32,
        ColumnType::NText => 33,
        ColumnType::SSVariant => 34,
    }
}

fn base_1900_01_01() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(1900, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn base_0001_01_01() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(1, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn datetime_to_epoch_millis(dt: tiberius::time::DateTime) -> i64 {
    let day_dt = base_1900_01_01() + Duration::days(dt.days() as i64);
    let nanos = (dt.seconds_fragments() as i64) * 1_000_000_000i64 / 300i64;
    (day_dt + Duration::nanoseconds(nanos)).and_utc().timestamp_millis()
}

fn smalldatetime_to_epoch_millis(dt: tiberius::time::SmallDateTime) -> i64 {
    let day_dt = base_1900_01_01() + Duration::days(dt.days() as i64);
    let nanos = (dt.seconds_fragments() as i64) * 1_000_000_000i64 / 300i64;
    (day_dt + Duration::nanoseconds(nanos)).and_utc().timestamp_millis()
}

fn date_to_epoch_millis(d: tiberius::time::Date) -> i64 {
    let dt = base_0001_01_01() + Duration::days(d.days() as i64);
    dt.and_utc().timestamp_millis()
}

fn time_to_millis_since_midnight(t: tiberius::time::Time) -> i64 {
    let scale = t.scale() as u32;
    // increments is 10^-scale seconds since midnight.
    let nanos = if scale <= 9 {
        let factor = 10u64.pow(9u32.saturating_sub(scale));
        (t.increments()).saturating_mul(factor)
    } else {
        0
    };
    (nanos / 1_000_000u64) as i64
}

fn datetime2_to_epoch_millis(dt: tiberius::time::DateTime2) -> i64 {
    let date = dt.date();
    let time = dt.time();

    let base = base_0001_01_01() + Duration::days(date.days() as i64);

    let scale = time.scale() as u32;
    let nanos = if scale <= 9 {
        let factor = 10u64.pow(9u32.saturating_sub(scale));
        (time.increments()).saturating_mul(factor)
    } else {
        0
    };

    (base + Duration::nanoseconds(nanos as i64))
        .and_utc()
        .timestamp_millis()
}

fn datetimeoffset_to_epoch_millis(dt: tiberius::time::DateTimeOffset) -> i64 {
    let naive_utc_millis = datetime2_to_epoch_millis(dt.datetime2());
    let offset_minutes = dt.offset() as i64;
    naive_utc_millis - offset_minutes * 60_000
}

