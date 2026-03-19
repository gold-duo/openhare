// ignore_for_file: constant_identifier_names

import 'dart:convert';

import 'package:collection/collection.dart';
import 'package:mssql/mssql.dart';
import 'package:sql_parser/parser.dart';
import 'package:uuid/uuid.dart';

import 'db_driver_conn_meta.dart';
import 'db_driver_interface.dart';
import 'db_driver_metadata.dart';

class MssqlQueryValue extends BaseQueryValue {
  final QueryValue _value;

  MssqlQueryValue(this._value);

  @override
  String? getString() {
    return switch (_value) {
      QueryValue_NULL() => null,
      QueryValue_Bytes(:final field0) =>
        utf8.decode(field0, allowMalformed: true),
      QueryValue_Int(:final field0) => field0.toString(),
      QueryValue_UInt(:final field0) => field0.toString(),
      QueryValue_Float(:final field0) => field0.toString(),
      QueryValue_Double(:final field0) => field0.toString(),
      QueryValue_DateTime(:final field0) => field0.toString(),
    };
  }

  @override
  List<int> getBytes() {
    return switch (_value) {
      QueryValue_Bytes(:final field0) => field0.toList(),
      _ => <int>[],
    };
  }
}

class MssqlQueryColumn extends BaseQueryColumn {
  final QueryColumn _column;

  MssqlQueryColumn(this._column);

  @override
  String get name => _column.name;

  // Must match rust `column_type_to_u8` mapping in `impl/mssql/rust/src/api/mssql.rs`
  static const TYPE_NULL = 0;
  static const TYPE_BIT = 1;
  static const TYPE_INT1 = 2;
  static const TYPE_INT2 = 3;
  static const TYPE_INT4 = 4;
  static const TYPE_INT8 = 5;
  static const TYPE_DATETIME4 = 6;
  static const TYPE_FLOAT4 = 7;
  static const TYPE_FLOAT8 = 8;
  static const TYPE_MONEY = 9;
  static const TYPE_DATETIME = 10;
  static const TYPE_MONEY4 = 11;
  static const TYPE_GUID = 12;
  static const TYPE_INTN = 13;
  static const TYPE_BITN = 14;
  static const TYPE_DECIMALN = 15;
  static const TYPE_NUMERICN = 16;
  static const TYPE_FLOATN = 17;
  static const TYPE_DATETIMEN = 18;
  static const TYPE_DATEN = 19;
  static const TYPE_TIMEN = 20;
  static const TYPE_DATETIME2 = 21;
  static const TYPE_DATETIMEOFFSETN = 22;
  static const TYPE_BIGVARBIN = 23;
  static const TYPE_BIGVARCHAR = 24;
  static const TYPE_BIGBINARY = 25;
  static const TYPE_BIGCHAR = 26;
  static const TYPE_NVARCHAR = 27;
  static const TYPE_NCHAR = 28;
  static const TYPE_XML = 29;
  static const TYPE_UDT = 30;
  static const TYPE_TEXT = 31;
  static const TYPE_IMAGE = 32;
  static const TYPE_NTEXT = 33;
  static const TYPE_SSVARIANT = 34;

  @override
  DataType dataType() {
    return switch (_column.columnType) {
      TYPE_XML => DataType.json,
      TYPE_BIGVARBIN ||
      TYPE_BIGBINARY ||
      TYPE_IMAGE ||
      TYPE_UDT ||
      TYPE_SSVARIANT =>
        DataType.blob,
      TYPE_BIGVARCHAR ||
      TYPE_BIGCHAR ||
      TYPE_NVARCHAR ||
      TYPE_NCHAR ||
      TYPE_TEXT ||
      TYPE_NTEXT ||
      TYPE_GUID =>
        DataType.char,
      TYPE_DATETIME4 ||
      TYPE_DATETIME ||
      TYPE_DATETIMEN ||
      TYPE_DATEN ||
      TYPE_TIMEN ||
      TYPE_DATETIME2 ||
      TYPE_DATETIMEOFFSETN =>
        DataType.time,
      TYPE_BIT || TYPE_BITN => DataType.dataSet,
      TYPE_INT1 ||
      TYPE_INT2 ||
      TYPE_INT4 ||
      TYPE_INT8 ||
      TYPE_INTN ||
      TYPE_FLOAT4 ||
      TYPE_FLOAT8 ||
      TYPE_FLOATN ||
      TYPE_MONEY ||
      TYPE_MONEY4 ||
      TYPE_DECIMALN ||
      TYPE_NUMERICN =>
        DataType.number,
      _ => DataType.char,
    };
  }
}

class MSSQLConnection extends BaseConnection {
  final ConnWrapper _conn;
  final String _dsn;
  String? _spid;

  MSSQLConnection(this._conn, this._dsn);

  static Future<BaseConnection> open(
      {required ConnectValue meta, String? schema}) async {
    final database = (schema != null && schema.isNotEmpty)
        ? schema
        : meta.getValue("database", "master");
    final encrypt = meta.getValue("encrypt", "true");
    final trustServerCertificate =
        meta.getValue("trustServerCertificate", "true");

    final dsn = Uri(
      scheme: "mssql",
      userInfo: '${meta.user}:${Uri.encodeComponent(meta.password)}',
      host: meta.getHost(),
      port: meta.getPort() ?? 1433,
      path: database,
      queryParameters: {
        "encrypt": encrypt,
        "trustServerCertificate": trustServerCertificate,
      },
    ).toString();

    final conn = await ConnWrapper.open(dsn: dsn);
    final mc = MSSQLConnection(conn, dsn);
    await mc._loadSpid();
    return mc;
  }

  Future<void> _loadSpid() async {
    final results = await query("SELECT @@SPID AS spid;");
    _spid = results.rows.firstOrNull?.getString("spid");
  }

  @override
  Future<void> close() async {
    await _conn.close();
    _conn.dispose();
  }

  @override
  Future<void> ping() async {
    await query("SELECT 1");
  }

  @override
  Future<void> killQuery() async {
    if (_spid == null || _spid!.isEmpty) return;

    // 新连接执行 KILL，最佳努力取消当前连接上的查询
    MSSQLConnection? tmp;
    try {
      final tmpConn = await ConnWrapper.open(dsn: _dsn);
      tmp = MSSQLConnection(tmpConn, _dsn);
      await tmp.query("KILL $_spid;");
    } finally {
      await tmp?.close();
    }
  }

  @override
  Future<BaseQueryResult> query(String sql, {int? limit}) async {
    final queryId = Uuid().v4();
    List<BaseQueryColumn>? resultColumns;
    BigInt? resultAffectedRows;
    final rows = <QueryResultRow>[];

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
      throw StateError('No header received');
    }

    return BaseQueryResult(queryId, resultColumns, rows, resultAffectedRows);
  }

  String _escapeIdent(String ident) {
    final escaped = ident.replaceAll(']', ']]');
    return '[$escaped]';
  }

  @override
  Stream<BaseQueryStreamItem> queryStream(String sql, {int? limit}) async* {
    final sd = parser(DialectType.mssql, sql);
    if (limit != null && sd.canLimit) {
      sql = sd.wrapLimit(limit: limit);
    }

    final queryId = Uuid().v4();
    sql = "/* call by openhare, uuid: $queryId */ $sql";

    List<BaseQueryColumn>? columns;

    await for (final item in _conn.query(query: sql)) {
      switch (item) {
        case QueryStreamItem_Header(:final field0):
          columns = field0.columns
              .map<BaseQueryColumn>((c) => MssqlQueryColumn(c))
              .toList();
          yield QueryStreamItemHeader(
            columns: columns,
            affectedRows: field0.affectedRows,
          );
        case QueryStreamItem_Row(:final field0):
          if (columns == null) {
            throw StateError('Received row before header');
          }
          yield QueryStreamItemRow(
            row: QueryResultRow(
              columns,
              field0.values.map((v) => MssqlQueryValue(v)).toList(),
            ),
          );
        case QueryStreamItem_Error(:final field0):
          throw Exception(field0);
      }
    }

    if (sd.changeSchema) {
      final currentSchema = await getCurrentSchema();
      onSchemaChanged(currentSchema ?? "");
    }
  }

  @override
  Future<String> version() async {
    final results = await query("SELECT @@VERSION AS version;");
    return results.rows.first.getString("version") ?? "";
  }

  @override
  Future<List<MetaDataNode>> metadata() async {
    final results = await query("""SELECT
    t.TABLE_CATALOG AS TABLE_SCHEMA,
    t.TABLE_NAME AS TABLE_NAME,
    c.COLUMN_NAME AS COLUMN_NAME,
    c.DATA_TYPE AS DATA_TYPE
FROM
    INFORMATION_SCHEMA.TABLES t
JOIN
    INFORMATION_SCHEMA.COLUMNS c
    ON t.TABLE_CATALOG = c.TABLE_CATALOG
    AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
    AND t.TABLE_NAME = c.TABLE_NAME
WHERE
    t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
ORDER BY
    t.TABLE_CATALOG,
    t.TABLE_NAME,
    c.ORDINAL_POSITION;""");

    final rows = results.rows;
    final schemaNodes = <MetaDataNode>[];

    final schemaRows =
        rows.groupListsBy((result) => result.getString("TABLE_SCHEMA")!);

    for (final schema in schemaRows.keys) {
      final schemaNode = MetaDataNode(MetaType.schema, schema);
      schemaNodes.add(schemaNode);

      final tableNodes = <MetaDataNode>[];
      final tableRows = schemaRows[schema]!
          .groupListsBy((result) => result.getString("TABLE_NAME")!);

      for (final table in tableRows.keys) {
        final tableNode = MetaDataNode(MetaType.table, table);
        tableNodes.add(tableNode);

        final columnRows = tableRows[table]!;
        final columnNodes = columnRows
            .map((result) =>
                MetaDataNode(MetaType.column, result.getString("COLUMN_NAME")!)
                  ..withProp(MetaDataPropType.dataType,
                      _getDataType(result.getString("DATA_TYPE")!)))
            .toList();
        tableNode.items = columnNodes;
      }

      schemaNode.items = tableNodes;
    }

    return schemaNodes;
  }

  static DataType _getDataType(String dataType) {
    final t = dataType.toLowerCase();
    return switch (t) {
      "int" ||
      "bigint" ||
      "smallint" ||
      "tinyint" ||
      "decimal" ||
      "numeric" ||
      "money" ||
      "smallmoney" ||
      "float" ||
      "real" =>
        DataType.number,
      "char" ||
      "varchar" ||
      "nchar" ||
      "nvarchar" ||
      "text" ||
      "ntext" ||
      "uniqueidentifier" =>
        DataType.char,
      "date" ||
      "time" ||
      "datetime" ||
      "smalldatetime" ||
      "datetime2" ||
      "datetimeoffset" =>
        DataType.time,
      "binary" || "varbinary" || "image" => DataType.blob,
      "xml" => DataType.json,
      "bit" => DataType.dataSet,
      _ => DataType.char,
    };
  }

  @override
  Future<List<String>> schemas() async {
    final results = await query(
        "SELECT name AS SCHEMA_NAME FROM sys.databases ORDER BY name;");
    return results.rows
        .map((r) => r.getString("SCHEMA_NAME") ?? "")
        .where((s) => s.isNotEmpty)
        .toList();
  }

  @override
  Future<void> setCurrentSchema(String schema) async {
    await query("USE ${_escapeIdent(schema)};");
    final currentSchema = await getCurrentSchema();
    onSchemaChanged(currentSchema ?? "");
  }

  @override
  Future<String?> getCurrentSchema() async {
    final results = await query("SELECT DB_NAME() AS CURRENT_SCHEMA;");
    return results.rows.first.getString("CURRENT_SCHEMA");
  }
}
