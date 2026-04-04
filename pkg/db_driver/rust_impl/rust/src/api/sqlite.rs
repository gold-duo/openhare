use crate::api::db::{DataType, QueryColumn, QueryHeader, QueryRow, QueryStreamItem, QueryValue};
use crate::frb_generated::StreamSink;
use rusqlite::Connection;
use std::sync::Mutex;

fn sqlite_type_to_data_type(type_name: &str) -> DataType {
    let t = type_name.to_uppercase();
    if t.contains("INT") || t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB") || t.contains("NUMERIC") || t.contains("DECIMAL") {
        return DataType::Number;
    }
    if t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT") {
        return DataType::Char;
    }
    if t.contains("DATE") || t.contains("TIME") {
        return DataType::Time;
    }
    if t.contains("BLOB") {
        return DataType::Blob;
    }
    if t.contains("JSON") {
        return DataType::Json;
    }
    DataType::Char
}

pub struct SqliteConnection {
    conn: Mutex<Connection>,
}

impl SqliteConnection {
    pub async fn open(dsn: &str) -> Result<Self, String> {
        let conn = Connection::open(dsn).map_err(|e| e.to_string())?;
        Ok(SqliteConnection {
            conn: Mutex::new(conn),
        })
    }

    pub async fn query(
        &mut self,
        query: &str,
        sink: StreamSink<QueryStreamItem>,
    ) -> Result<(), String> {
        let query = query.trim();
        if query.is_empty() {
            let _ = sink.add(QueryStreamItem::Header(QueryHeader {
                columns: vec![],
                affected_rows: 0,
            }));
            return Ok(());
        }

        let conn = self.conn.lock().map_err(|e| e.to_string())?;

        let mut stmt = match conn.prepare(query) {
            Ok(stmt) => stmt,
            Err(e) => {
                let _ = sink.add(QueryStreamItem::Error(e.to_string()));
                return Ok(());
            }
        };

        let column_count = stmt.column_count();
        if column_count == 0 {
            drop(stmt);
            match conn.execute(query, []) {
                Ok(affected_rows) => {
                    let _ = sink.add(QueryStreamItem::Header(QueryHeader {
                        columns: vec![],
                        affected_rows: affected_rows as u64,
                    }));
                }
                Err(e) => {
                    let _ = sink.add(QueryStreamItem::Error(e.to_string()));
                }
            }
            return Ok(());
        }

        let cols = stmt.columns();
        let mut columns = Vec::with_capacity(column_count);
        for col in cols {
            let name = col.name().to_string();
            let data_type = col.decl_type()
                .map(|t| sqlite_type_to_data_type(t))
                .unwrap_or(DataType::Char);
            columns.push(QueryColumn { name, data_type });
        }

        if sink
            .add(QueryStreamItem::Header(QueryHeader {
                columns,
                affected_rows: 0,
            }))
            .is_err()
        {
            return Ok(());
        }

        let mut rows = match stmt.query([]) {
            Ok(rows) => rows,
            Err(e) => {
                let _ = sink.add(QueryStreamItem::Error(e.to_string()));
                return Ok(());
            }
        };

        while let Some(row) = rows.next().map_err(|e| e.to_string())? {
            let mut values = Vec::with_capacity(column_count);
            for i in 0..column_count {
                let value = row.get_ref(i).map_err(|e| e.to_string())?;
                let query_value = match value {
                    rusqlite::types::ValueRef::Null => QueryValue::NULL,
                    rusqlite::types::ValueRef::Integer(v) => QueryValue::Int(v),
                    rusqlite::types::ValueRef::Real(v) => QueryValue::Double(v),
                    rusqlite::types::ValueRef::Text(v) => QueryValue::Bytes(v.to_vec()),
                    rusqlite::types::ValueRef::Blob(v) => QueryValue::Bytes(v.to_vec()),
                };
                values.push(query_value);
            }

            if sink.add(QueryStreamItem::Row(QueryRow { values })).is_err() {
                return Ok(());
            }
        }

        Ok(())
    }

    pub async fn close(self) -> Result<(), String> {
        Ok(())
    }
}
