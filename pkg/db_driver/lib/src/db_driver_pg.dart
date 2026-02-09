import 'package:collection/collection.dart';
import 'package:pg/pg.dart';
import 'package:sql_parser/parser.dart';
import 'package:uuid/uuid.dart';
import 'db_driver_interface.dart';
import 'db_driver_conn_meta.dart';
import 'db_driver_metadata.dart';

class PGQueryValue extends BaseQueryValue {
  final Object? _value;

  PGQueryValue(this._value);

  @override
  String? getString() {
    if (_value == null) {
      return null;
    }
    return _value.toString();
  }

  @override
  List<int> getBytes() {
    // todo
    return List.empty();
  }
}

class PGQueryColumn extends BaseQueryColumn {
  final ResultSchemaColumn _column;

  PGQueryColumn(this._column);

  @override
  String get name => _column.columnName ?? "";

  @override
  DataType dataType() {
    return switch (_column.type) {
      Type.json || Type.jsonb || Type.jsonbArray => DataType.json, // JSON

      Type.text ||
      Type.textArray ||
      Type.byteArray =>
        DataType.blob, // BLOB, TINY_BLOB, MEDIUM_BLOB, LONG_BLOB

      Type.character ||
      Type.varChar ||
      Type.varCharArray =>
        DataType.char, // STRING, VARCHAR, VAR_STRING

      Type.time ||
      Type.timeArray ||
      Type.timestamp ||
      Type.timestampArray ||
      Type.timestampRange ||
      Type.timestampTz ||
      Type.timestampTzArray ||
      Type.timestampRange ||
      Type.timestampWithTimezone ||
      Type.timestampWithoutTimezone ||
      Type.date ||
      Type.dateArray ||
      Type.dateRange =>
        DataType.time, // DATE, DATETIME, TIMESTAMP

      Type.bigInteger ||
      Type.bigIntegerArray ||
      Type.bigIntegerRange ||
      Type.bigSerial ||
      Type.integer ||
      Type.integerArray ||
      Type.integerRange ||
      Type.smallInteger ||
      Type.smallIntegerArray =>
        DataType.number, // number

      _ => DataType.char,
    };
  }
}

class PGConnection extends BaseConnection {
  final PGConn _conn;
  final ConnectValue _meta;
  int? _backendPid;

  PGConnection(this._conn, this._meta);

  static Future<BaseConnection> open(
      {required ConnectValue meta, String? schema}) async {
    final conn = await PGConn.open(
      endpoint: Endpoint(
        host: meta.host,
        port: meta.port ?? 5432,
        password: meta.password,
        username: meta.user,
        database: meta.getValue("database", "postgres"),
      ),
      connectTimeout: Duration(seconds: meta.getIntValue("connectTimeout", 10)),
      queryTimeout: Duration(seconds: meta.getIntValue("queryTimeout", 600)),
    );

    final pgConn = PGConnection(conn, meta);
    await pgConn._loadBackendPid();
    if (schema != null) {
      pgConn.setCurrentSchema(schema);
    }
    return pgConn;
  }

  Future<void> _loadBackendPid() async {
    final results = await query("SELECT pg_backend_pid() AS pid");
    _backendPid = int.tryParse(results.rows.first.getString("pid") ?? "");
  }

  @override
  Future<void> ping() async {
    await query("SELECT 1");
  }

  @override
  Future<String> version() async {
    final results = await query("SELECT version() AS version");
    final rows = results.rows;
    return rows.first.getString("version") ?? "";
  }

  @override
  Future<void> killQuery() async {
    if (_backendPid == null) return;
    // 使用新连接取消当前连接上运行的查询
    PGConn? tmp;
    try {
      tmp = await PGConn.open(
        endpoint: Endpoint(
          host: _meta.host,
          port: _meta.port ?? 5432,
          password: _meta.password,
          username: _meta.user,
          database: _meta.getValue("database", "postgres"),
        ),
        connectTimeout:
            Duration(seconds: _meta.getIntValue("connectTimeout", 10)),
        queryTimeout: Duration(seconds: _meta.getIntValue("queryTimeout", 600)),
      );
      await tmp.query(query: "SELECT pg_cancel_backend($_backendPid)");
    } finally {
      if (tmp != null) await tmp.close();
    }
  }

  @override
  Future<BaseQueryResult> query(String sql, {int? limit}) async {
    final queryId = Uuid().v4();
    List<BaseQueryColumn>? resultColumns;
    BigInt? resultAffectedRows;
    List<QueryResultRow> rows = [];
    
    await for (final item in queryStream(sql, limit: limit)) {
      switch (item) {
        case QueryStreamItemHeader(:final columns, :final affectedRows):
          resultColumns = columns;
          resultAffectedRows = affectedRows;
        case QueryStreamItemRow(:final row):
          rows.add(row);
      }
    }
    
    if (resultColumns == null || resultAffectedRows == null) {
      throw StateError('No metadata received');
    }
    
    return BaseQueryResult(queryId, resultColumns, rows, resultAffectedRows);
  }

  String _wrapLimit(String sql, int limit) {
    sql = sql.trimRight();
    if (sql.endsWith(";")) sql = sql.substring(0, sql.length - 1);
    return "SELECT * FROM ($sql) AS dt_1 LIMIT $limit;";
  }

  @override
  Stream<BaseQueryStreamItem> queryStream(String sql, {int? limit}) async* {
    final firstTok = Lexer(sql).firstTrim();
    if (limit != null &&
        firstTok != null &&
        firstTok.content.toLowerCase() == "select") {
      sql = _wrapLimit(sql, limit);
    }
    List<BaseQueryColumn>? columns;
    await for (final item in _conn.queryStream(query: sql)) {
      switch (item) {
        case PGStreamHeader(:final schema, :final affectedRows):
          columns = schema.columns
              .map<PGQueryColumn>((c) => PGQueryColumn(c))
              .toList();
          yield QueryStreamItemHeader(
            columns: columns,
            affectedRows: BigInt.from(affectedRows),
          );
        case ResultRow row when columns != null:
          yield QueryStreamItemRow(
            row: QueryResultRow(
              columns,
              row.map((v) => PGQueryValue(v)).toList(),
            ),
          );
        case ResultRow():
          throw StateError('Received row before header');
      }
    }
  }

  @override
  Future<void> close() async {
    await _conn.close();
  }

  @override
  Future<List<MetaDataNode>> metadata() async {
    // ref: https://www.postgresql.org/docs/current/information-schema.html
    final results = await query("""SELECT 
    t.table_schema,
    t.table_name,
    c.column_name,
    c.data_type
FROM 
    information_schema.tables t
JOIN 
    information_schema.columns c 
    ON t.table_name = c.table_name 
    AND t.table_schema = c.table_schema
WHERE 
    t.table_type IN ('BASE TABLE', 'VIEW')
ORDER BY
    t.table_schema,
    t.table_name, 
    c.ordinal_position;
""");
    final rows = results.rows;
    List<MetaDataNode> schemaNodes = List.empty(growable: true);
    // group by Schema Name
    final schemaRows =
        rows.groupListsBy((result) => result.getString("table_schema")!);

    for (final schema in schemaRows.keys) {
      final schemaNode = MetaDataNode(MetaType.schema, schema);
      schemaNodes.add(schemaNode);

      // group by Table Name
      List<MetaDataNode> tableNodes = List.empty(growable: true);
      final tableRows = schemaRows[schema]!
          .groupListsBy((result) => result.getString("table_name")!);
      for (final table in tableRows.keys) {
        final tableNode = MetaDataNode(MetaType.table, table);
        tableNodes.add(tableNode);

        // handler columns
        final columnRows = tableRows[table]!;
        final columnNodes = columnRows
            .map((result) => MetaDataNode(MetaType.column,
                    result.getString("column_name")!)
                ..withProp(MetaDataPropType.dataType,
                    _getDataType(result.getString("data_type")!)))
            .toList();
        tableNode.items = columnNodes;
      }
      schemaNode.items = tableNodes;
    }
    return schemaNodes;
  }

  static DataType _getDataType(String dataType) {
    return switch (dataType) {
      "integer" ||
      "bigint" ||
      "smallint" ||
      "serial" ||
      "bigserial" ||
      "smallserial" ||
      "numeric" ||
      "decimal" ||
      "real" ||
      "double precision" =>
        DataType.number,
      "character varying" || "varchar" || "character" || "char" || "text" =>
        DataType.char,
      "timestamp without time zone" ||
      "timestamp with time zone" ||
      "timestamptz" ||
      "timestamp" ||
      "date" ||
      "time without time zone" ||
      "time with time zone" ||
      "time" ||
      "interval" =>
        DataType.time,
      "bytea" => DataType.blob,
      "json" || "jsonb" => DataType.json,
      "boolean" => DataType.dataSet,
      "uuid" => DataType.char,
      "array" || "USER-DEFINED" => DataType.blob,
      _ => DataType.char,
    };
  }

  @override
  Future<void> setCurrentSchema(String schema) async {
    await query("SET search_path TO $schema");
    final currentSchema = await getCurrentSchema();
    onSchemaChanged(currentSchema!);
    return;
  }

  @override
  Future<String?> getCurrentSchema() async {
    final results = await query("SELECT current_schema();");
    final rows = results.rows;
    final currentSchema = rows.first.getString("current_schema");
    return currentSchema;
  }

  @override
  Future<List<String>> schemas() async {
    List<String> schemas = List.empty(growable: true);
    final results = await query("SELECT nspname FROM pg_namespace;");
    final rows = results.rows;
    for (final result in rows) {
      schemas.add(result.getString("nspname") ?? "");
    }
    return schemas;
  }
}
