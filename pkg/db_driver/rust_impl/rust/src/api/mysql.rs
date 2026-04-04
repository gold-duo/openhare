use crate::api::db::{DataType, QueryColumn, QueryHeader, QueryRow, QueryStreamItem, QueryValue};
use crate::frb_generated::StreamSink;
use chrono::NaiveDate;
use mysql_async::{prelude::*, Conn, Opts};

#[allow(non_camel_case_types)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MySqlColumnType {
    MYSQL_TYPE_DECIMAL = 0,
    MYSQL_TYPE_TINY = 1,
    MYSQL_TYPE_SHORT = 2,
    MYSQL_TYPE_LONG = 3,
    MYSQL_TYPE_FLOAT = 4,
    MYSQL_TYPE_DOUBLE = 5,
    MYSQL_TYPE_NULL = 6,
    MYSQL_TYPE_TIMESTAMP = 7,
    MYSQL_TYPE_LONGLONG = 8,
    MYSQL_TYPE_INT24 = 9,
    MYSQL_TYPE_DATE = 10,
    MYSQL_TYPE_TIME = 11,
    MYSQL_TYPE_DATETIME = 12,
    MYSQL_TYPE_YEAR = 13,
    MYSQL_TYPE_NEWDATE = 14,
    MYSQL_TYPE_VARCHAR = 15,
    MYSQL_TYPE_BIT = 16,
    MYSQL_TYPE_TIMESTAMP2 = 17,
    MYSQL_TYPE_DATETIME2 = 18,
    MYSQL_TYPE_TIME2 = 19,
    MYSQL_TYPE_TYPED_ARRAY = 20,
    MYSQL_TYPE_VECTOR = 242,
    MYSQL_TYPE_UNKNOWN = 243,
    MYSQL_TYPE_JSON = 245,
    MYSQL_TYPE_NEWDECIMAL = 246,
    MYSQL_TYPE_ENUM = 247,
    MYSQL_TYPE_SET = 248,
    MYSQL_TYPE_TINY_BLOB = 249,
    MYSQL_TYPE_MEDIUM_BLOB = 250,
    MYSQL_TYPE_LONG_BLOB = 251,
    MYSQL_TYPE_BLOB = 252,
    MYSQL_TYPE_VAR_STRING = 253,
    MYSQL_TYPE_STRING = 254,
    MYSQL_TYPE_GEOMETRY = 255,
}

impl From<u8> for MySqlColumnType {
    fn from(value: u8) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

fn mysql_type_to_data_type(column_type_value: u8) -> DataType {
    let col_type = MySqlColumnType::from(column_type_value);
    match col_type {
        MySqlColumnType::MYSQL_TYPE_JSON => DataType::Json,
        MySqlColumnType::MYSQL_TYPE_BIT |
        MySqlColumnType::MYSQL_TYPE_TINY_BLOB |
        MySqlColumnType::MYSQL_TYPE_MEDIUM_BLOB |
        MySqlColumnType::MYSQL_TYPE_LONG_BLOB |
        MySqlColumnType::MYSQL_TYPE_BLOB => DataType::Blob,
        MySqlColumnType::MYSQL_TYPE_VARCHAR |
        MySqlColumnType::MYSQL_TYPE_VAR_STRING |
        MySqlColumnType::MYSQL_TYPE_STRING => DataType::Char,
        MySqlColumnType::MYSQL_TYPE_TIMESTAMP |
        MySqlColumnType::MYSQL_TYPE_DATE |
        MySqlColumnType::MYSQL_TYPE_TIME |
        MySqlColumnType::MYSQL_TYPE_DATETIME |
        MySqlColumnType::MYSQL_TYPE_YEAR |
        MySqlColumnType::MYSQL_TYPE_TIMESTAMP2 |
        MySqlColumnType::MYSQL_TYPE_DATETIME2 |
        MySqlColumnType::MYSQL_TYPE_TIME2 => DataType::Time,
        MySqlColumnType::MYSQL_TYPE_DECIMAL |
        MySqlColumnType::MYSQL_TYPE_TINY |
        MySqlColumnType::MYSQL_TYPE_SHORT |
        MySqlColumnType::MYSQL_TYPE_LONG |
        MySqlColumnType::MYSQL_TYPE_FLOAT |
        MySqlColumnType::MYSQL_TYPE_DOUBLE |
        MySqlColumnType::MYSQL_TYPE_LONGLONG |
        MySqlColumnType::MYSQL_TYPE_INT24 |
        MySqlColumnType::MYSQL_TYPE_NEWDECIMAL => DataType::Number,
        MySqlColumnType::MYSQL_TYPE_ENUM |
        MySqlColumnType::MYSQL_TYPE_SET => DataType::DataSet,
        _ => DataType::Char,
    }
}

pub struct MySqlConnection {
    conn: Conn,
}

impl MySqlConnection {
    pub async fn open(dsn: &str) -> Result<Self, String> {
        let opts = Opts::from_url(dsn).map_err(|e| e.to_string())?;
        let conn = Conn::new(opts).await.map_err(|e| e.to_string())?;
        Ok(MySqlConnection { conn })
    }

    pub async fn query(
        &mut self,
        query: &str,
        sink: StreamSink<QueryStreamItem>,
    ) -> Result<(), String> {
        let mut result_set = match self.conn.query_iter(query).await {
            Ok(rs) => rs,
            Err(e) => {
                let _ = sink.add(QueryStreamItem::Error(format!("{}", e)));
                return Ok(());
            }
        };

        let columns = match result_set.columns() {
            Some(cols) => cols
                .iter()
                .map(|col| QueryColumn {
                    name: col.name_str().to_string(),
                    data_type: mysql_type_to_data_type(col.column_type() as u8),
                })
                .collect(),
            None => {
                let _ = sink.add(QueryStreamItem::Error("Failed to fetch column".to_string()));
                return Ok(());
            }
        };

        let affected_rows = result_set.affected_rows();

        let header = QueryHeader {
            columns,
            affected_rows,
        };
        if let Err(e) = sink.add(QueryStreamItem::Header(header)) {
            let _ = sink.add(QueryStreamItem::Error(format!("Failed to send header: {}", e)));
            return Ok(());
        }

        while let Some(row_result) = result_set.next().await.transpose() {
            match row_result {
                Ok(row) => {
                    let values = row
                        .unwrap()
                        .into_iter()
                        .map(|v| match v {
                            mysql_async::Value::Int(i) => QueryValue::Int(i),
                            mysql_async::Value::UInt(u) => QueryValue::UInt(u),
                            mysql_async::Value::Float(f) => QueryValue::Float(f),
                            mysql_async::Value::Double(d) => QueryValue::Double(d),
                            mysql_async::Value::Bytes(b) => QueryValue::Bytes(b),
                            mysql_async::Value::Date(y, m, d, h, min, s, micros) => {
                                let dt = NaiveDate::from_ymd_opt(y as i32, m as u32, d as u32)
                                    .unwrap()
                                    .and_hms_micro_opt(h as u32, min as u32, s as u32, micros as u32)
                                    .unwrap();
                                let timestamp = dt.and_utc().timestamp_millis();
                                QueryValue::DateTime(timestamp)
                            }
                            mysql_async::Value::Time(is_neg, d, h, min, s, micros) => {
                                let total_seconds = (d as i64) * 86400 * 1000
                                    + (h as i64) * 3600 * 1000
                                    + (min as i64) * 60 * 1000
                                    + (s as i64) * 1000
                                    + micros as i64;
                                let timestamp = if is_neg { -total_seconds } else { total_seconds };
                                QueryValue::DateTime(timestamp)
                            }
                            mysql_async::Value::NULL => QueryValue::NULL,
                        })
                        .collect();
                    let query_row = QueryRow { values };
                    if let Err(e) = sink.add(QueryStreamItem::Row(query_row)) {
                        let _ = sink.add(QueryStreamItem::Error(format!("Failed to send row: {}", e)));
                        return Ok(());
                    }
                }
                Err(e) => {
                    let _ = sink.add(QueryStreamItem::Error(format!("{}", e)));
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    pub async fn close(self) -> Result<(), String> {
        self.conn.disconnect().await.map_err(|e| e.to_string())
    }
}
