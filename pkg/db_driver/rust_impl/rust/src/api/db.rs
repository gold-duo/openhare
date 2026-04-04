pub enum DataType {
    Number,
    Char,
    Time,
    Blob,
    Json,
    DataSet,
}

pub enum QueryValue {
    NULL,
    Bytes(Vec<u8>),
    Int(i64),
    UInt(u64),
    Float(f32),
    Double(f64),
    DateTime(i64),
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
    pub data_type: DataType,
}

pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

pub enum DbType {
    MySQL,
    SQLite,
}

#[flutter_rust_bridge::frb(init)]
pub fn init_app() {
    flutter_rust_bridge::setup_default_user_utils();
}
